// cc-scanner impfmt.Errorf("HTTP error for %s: %v", url, err)orts domains from the Common Crawl URL index
//
// LICENCE: No licence is provided for this project

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"golang.org/x/net/publicsuffix"
	"golang.org/x/time/rate"
)

const (
	crawlIndexUrl = "http://index.commoncrawl.org/collinfo.json"
	maxRetries    = 360
	retryInterval = time.Minute
)

var (
	db       *sqlx.DB
	cl       *http.Client
	throttle *rate.Limiter
)

func main() {
	var err error
	db, err = sqlx.Open("postgres", os.Getenv("SCANNER_DSN"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	log.Println("Updating schema ...")
	if err := ensureSchema(); err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	cl = &http.Client{
		Timeout: 30 * time.Minute,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 60 * time.Second,
			DisableKeepAlives:     false,
			MaxIdleConns:          10,
			IdleConnTimeout:       90 * time.Second,
		},
	}

	throttle = rate.NewLimiter(rate.Limit(5), 1)

	for {
		log.Println("Fetching Common Crawl index list ...")
		if err := updateCrawlIndexes(); err != nil {
			log.Fatalf("Failed to update index list: %v", err)
		}

		crawl()

		time.Sleep(time.Hour)
	}

}

func crawl() {
	var toCrawl []struct {
		Name string `db:"name"`
		URL  string `db:"url"`
	}
	if err := db.Select(&toCrawl, "SELECT name, url FROM crawl_indexes WHERE crawled = false;"); err != nil {
		log.Fatalf("Failed to query for indexes to crawl: %v", err)
	}

	log.Printf("Will launch %d crawlers ...", len(toCrawl))

	var wg sync.WaitGroup
	for _, idx := range toCrawl {
		wg.Add(1)
		go func(id string, url string) {
			defer wg.Done()
			if err := crawlIndex(id, url); err != nil {
				log.Printf("Crawler %s fatal error: %v", id, err)
			}
		}(idx.Name, idx.URL)
	}

	wg.Wait()

	log.Println("All crawlers done")
}

func crawlIndex(id string, indexUrl string) error {
	// First we get number of pages
	params := url.Values{}
	params.Add("url", "*.au")
	params.Add("output", "json")
	params.Add("fl", "timestamp,url")
	// params.Add("filter", `url:.*/robots.txt$`)
	params.Add("pageSize", "1")
	params.Add("showNumPages", "true")
	resp, err := fetchWithRetry(fmt.Sprintf(`%s?&&/?%s`, indexUrl, params.Encode()))
	if err != nil {
		return err
	}

	var stats struct {
		Pages int `json:"pages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		resp.Body.Close()
		return fmt.Errorf("Couldn't decode page count: %v", err)
	}

	log.Printf("%s has %d pages to fetch", id, stats.Pages)

	params.Del("showNumPages")
	params.Set("output", "text")

	for page := 0; page < stats.Pages; page++ {
		all := map[string]int64{}

		params.Set("page", strconv.Itoa(page))
		resp, err := fetchWithRetry(fmt.Sprintf(`%s?%s`, indexUrl, params.Encode()))
		if err != nil {
			return fmt.Errorf("Failed to fetch page %d: %v", page, err)
		}

		scanner := bufio.NewScanner(resp.Body)
		var s string
		for scanner.Scan() {
			s = scanner.Text()
			entry := strings.Split(s, " ")

			t, err := time.Parse("20060102150405", entry[0])
			if err != nil {
				log.Printf("Bad time format, ignoring (%v): %v", s, err)
				continue
			}
			u, err := url.Parse(entry[1])
			if err != nil {
				log.Printf("Bad URL format, ignoring (%v): %v", s, err)
				continue
			}

			d := u.Host
			if i := strings.Index(d, ":"); i >= 0 {
				d = d[:i]
			}
			d = strings.ToLower(strings.TrimSpace(strings.TrimSuffix(d, ".")))

			all[d] = t.Unix()
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Failed to scan response (%s page %d): %v, last line was: %s", id, page, err, s)
		}
		resp.Body.Close()

		submitNames(all)
	}

	if _, err := db.Exec("UPDATE crawl_indexes SET crawled = TRUE WHERE name = $1;", id); err != nil {
		log.Printf("Failed to update index %s to crawled: %v", id, err)
	}

	return nil
}

func fetchWithRetry(url string) (*http.Response, error) {
	ctx := context.Background()
	for attempts := 0; attempts < maxRetries; attempts++ {
		throttle.Wait(ctx)

		resp, err := cl.Get(url)
		if err == nil && resp != nil && resp.StatusCode == 200 {
			return resp, nil
		}

		log.Printf("HTTP fetch failed for %s (attempt %d): %v", url, attempts, err)
		time.Sleep(retryInterval)
	}

	return nil, errors.New("Ran out of attempts")
}

func submitNames(domains map[string]int64) {
	for name, ts := range domains {
		etld, err := publicsuffix.EffectiveTLDPlusOne(name)
		if err != nil {
			log.Printf("Couldn't determine etld for %s: %v", name, err)
		}

		if _, err := db.Exec(`INSERT INTO domains (domain, first_seen, last_seen, etld) VALUES ($1, $2, $2, $3) ON CONFLICT (domain) DO UPDATE SET last_seen = GREATEST($2,domains.last_seen), first_seen = LEAST(domains.first_seen, $2);`, name, ts, etld); err != nil {
			log.Printf("Failed to insert/update %s: %v", name, err)
		}
	}
}

func ensureSchema() error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS crawl_indexes (name varchar(255) PRIMARY KEY, crawled boolean, url varchar(2048) UNIQUE NOT NULL);`,
	}

	tx := db.MustBegin()
	defer tx.Rollback()

	for _, s := range schema {
		if _, err := tx.Exec(s); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func updateCrawlIndexes() error {
	resp, err := cl.Get(crawlIndexUrl)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 || resp.Header.Get("content-type") != "application/json" {
		return fmt.Errorf("unexpected response from crawl index list: %v", resp.StatusCode)
	}

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var out []struct {
		ID  string `json:"id"`
		URL string `json:"cdx-api"`
	}
	if err := json.Unmarshal(buf, &out); err != nil {
		return err
	}

	tx := db.MustBegin()
	defer tx.Rollback()

	for _, idx := range out {
		if _, err := tx.Exec(`INSERT INTO crawl_indexes (name, url, crawled) VALUES ($1, $2, false) ON CONFLICT DO NOTHING;`, idx.ID, idx.URL); err != nil {
			return err
		}
	}

	return tx.Commit()
}
