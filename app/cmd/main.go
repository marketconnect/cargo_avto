package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/chromedp/chromedp"
	_ "modernc.org/sqlite"
)

const (
	WBAPINUrl    = "https://marketplace-api.wildberries.ru/api/v3/stocks/%d"
	WarehouseID  = 1283008
	BatchSize    = 1000
	RequestLimit = 300 // 300 запросов в минуту

)

var bubblebagsURLMap = make(map[string]string)

func main() {
	apiKey := os.Getenv("WB_API_KEY")
	if apiKey == "" {
		log.Fatal("Перед запуском необходимо установить переменную окружения API_KEY")
	}
	if err := loadBubblebagsCSV(); err != nil {
		log.Fatalf("Ошибка загрузки URL из CSV: %v", err)
	}

	cfg := Config{
		ObjectIDs: []int{3979, 3756},

		DBName: "unit_ec.db",
		VendorCodePatterns: []string{
			"^box_\\d+_\\d+$",
			"^bubblebags_9\\d+_\\d+$",
			"^bubblebags_1\\d+_\\d+$",
		},
		UsePcs: true,
	}

	err := Process(apiKey, cfg)
	if err != nil {
		log.Fatalf("Ошибка при обработке: %v", err)
	}

	err = updateStocks(apiKey, cfg)
	if err != nil {
		log.Fatalf("Ошибка при обновлении стоки: %v", err)
	}
}

func loadBubblebagsCSV() error {
	file, err := os.Open("urls.csv")
	if err != nil {
		return fmt.Errorf("ошибка при открытии файла urls.csv: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) == 2 {
			// Пример: "bubblebags_19323,https://packio.ru/product/paket..."
			bubblebagsURLMap[parts[0]] = parts[1]
		}
	}
	return scanner.Err()
}

func updateStocks(apiKey string, cfg Config) error {
	db, err := sql.Open("sqlite", cfg.DBName)
	if err != nil {
		return fmt.Errorf("ошибка при открытии базы данных: %v", err)
	}
	defer db.Close()

	query := `
        SELECT sku, pcs, available_count
        FROM products
        WHERE sku IS NOT NULL
    `
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("ошибка при запросе к БД: %v", err)
	}
	defer rows.Close()

	var stocksData []stockItem

	for rows.Next() {
		var (
			skus           string
			pcs            int
			availableCount int
		)
		if err := rows.Scan(&skus, &pcs, &availableCount); err != nil {
			log.Printf("Ошибка чтения строки: %v", err)
			continue
		}

		amount := calcAmount(pcs, availableCount)
		item := stockItem{
			SKU:    skus,
			Amount: amount,
		}
		stocksData = append(stocksData, item)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Ошибка при чтении строк из БД: %v", err)
	}

	// Интервал между запросами (для соблюдения 300 в минуту)
	requestInterval := time.Duration(float64(time.Minute) / float64(RequestLimit))

	// 4) Отправляем запросы по BATCH_SIZE = 1000
	client := &http.Client{}
	total := len(stocksData)
	log.Printf("Всего товаров для отправки: %d\n", total)

	for i := 0; i < total; i += BatchSize {
		end := i + BatchSize
		if end > total {
			end = total
		}
		batch := stocksData[i:end]

		// Формируем JSON
		payload := stockRequest{Stocks: batch}
		jsonBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Ошибка маршалинга JSON: %v\n", err)
			continue
		}

		// Создаём PUT-запрос
		url := fmt.Sprintf(WBAPINUrl, WarehouseID)
		req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(string(jsonBytes)))
		if err != nil {
			log.Printf("Ошибка создания запроса: %v\n", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("❌ Ошибка при отправке запроса: %v\n", err)
			time.Sleep(requestInterval)
			continue
		}

		// Считываем статус
		if resp.StatusCode == http.StatusNoContent {
			// 204
			log.Printf("✅ Успешно обновлены остатки для %d товаров\n", len(batch))
		} else {
			// В случае ошибки читаем тело ответа (по желанию), но здесь просто выведем Status
			b, _ := ioutil.ReadAll(resp.Body)
			log.Printf("❌ Ошибка при обновлении: статус %d  тело ответа: %s\n", resp.StatusCode, string(b))
		}
		resp.Body.Close()

		// 6) Пауза, чтобы не превысить лимит
		time.Sleep(requestInterval)
	}

	log.Println("Готово!")
	return nil
}

type Config struct {
	ObjectIDs []int // SubjectIDs

	DBName             string   // DBName (for example, "ue.db")
	VendorCodePatterns []string // VendorCodePattern (for example, "^box_\d+_\d+$")
	UsePcs             bool     // UsePcs (for example, true)
}

const baseURL = "https://sp.cargo-avto.ru/catalog/"

func Process(apiKey string, cfg Config) error {

	if err := os.Remove(cfg.DBName); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("ошибка удаления старой базы данных: %v", err)
	}
	log.Println("Старая база данных удалена.")

	db, err := sql.Open("sqlite", cfg.DBName)
	if err != nil {
		return fmt.Errorf("ошибка при открытии базы данных: %v", err)
	}
	defer db.Close()

	createTable(db)

	// 3. Загружаем карточки, используя переданные objectIDs
	allCards := fetchAllCards(apiKey, cfg.ObjectIDs)
	log.Printf("Всего загружено %d карточек.", len(allCards))

	// 4. Настраиваем Chromedp для парсинга страниц
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", false),
		chromedp.Flag("disable-gpu", true),
	)
	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()

	ctx, ctxCancel := chromedp.NewContext(allocCtx)
	defer ctxCancel()

	productDataCache := make(map[string]map[string]string)
	skuMap := extractSKUs(allCards)
	// vendorCodePattern := regexp.MustCompile(cfg.VendorCodePattern)
	// 7. Обрабатываем каждую карточку
	for _, card := range allCards {
		var matched bool
		for _, pattern := range cfg.VendorCodePatterns {
			if regexp.MustCompile(pattern).MatchString(card.VendorCode) {
				matched = true
				break
			}
		}
		if !matched {
			log.Printf("Пропускаем товар с некорректным VendorCode: %s", card.VendorCode)
			continue
		}

		skus := skuMap[card.NmID]
		if len(skus) != 1 {
			panic(fmt.Sprintf("SKU либо отсутствует, либо их больше 1 для товара с VendorCode: %s", card.VendorCode))
		}

		// Извлекаем productID и pcs из vendorCode
		parts := strings.Split(card.VendorCode, "_")
		if len(parts) < 2 {
			log.Printf("Некорректный VendorCode: %s", card.VendorCode)
			continue
		}
		productID := parts[1]
		pcsInt := 1
		if len(parts) > 2 && cfg.UsePcs {
			if val, err := strconv.Atoi(parts[2]); err == nil {
				pcsInt = val
			}
		}

		// Парсинг данных товара (с кешированием)
		var productData map[string]string
		if cachedData, exists := productDataCache[productID]; exists {
			log.Printf("Используем кешированные данные для товара: %s", productID)
			productData = cachedData
		} else {
			log.Printf("Парсим страницу для товара: %s", productID)
			// url := baseURL + productID + "/"
			// productData, err = scrapeProductData(ctx, url)
			productData, err = scrapeProductData(ctx, card.VendorCode)
			if err != nil {
				log.Printf("Ошибка при обработке товара %s: %v", productID, err)
				continue
			}
			productDataCache[productID] = productData
		}

		// Рассчитываем стоимость с учетом количества pcs
		cost, err := convertAndMultiply(productData["price"], fmt.Sprintf("%d", pcsInt))
		if err != nil {
			log.Printf("Ошибка при конвертации и умножении для %s: %v", productID, err)
			continue
		}

		saveToDatabase(db, SaveParams{
			NmID:       card.NmID,
			VendorCode: card.VendorCode,

			Pcs:       pcsInt,
			ProductID: productID,

			AvailableCountStr: productData["availableCount"],
			Cost:              cost,
		}, skus[0])
	}

	log.Println("Обработка завершена.")
	return nil
}

func createTable(db *sql.DB) {
	query := `
	CREATE TABLE IF NOT EXISTS products (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		nm_id INTEGER,
		vendor_code TEXT,
		pcs INTEGER,
		product_id TEXT,
		sku TEXT,
		available_count INTEGER,
		cost INTEGER,
		UNIQUE (product_id, pcs)
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Ошибка при создании таблицы: %v", err)
	}
	log.Println("Таблица products проверена/создана.")
}

func fetchAllCards(apiKey string, objectIDs []int) []Card {
	var allCards []Card
	var updatedAt string
	var nmID int

	for {
		response, err := getCardsList(apiKey, updatedAt, nmID, objectIDs)
		if err != nil {
			log.Printf("Ошибка запроса карточек: %v", err)
			break
		}
		if response == nil || len(response.Cards) == 0 {
			log.Println("Больше нет карточек для загрузки.")
			break
		}
		allCards = append(allCards, response.Cards...)
		updatedAt = response.Cursor.UpdatedAt
		nmID = response.Cursor.NmID

		if updatedAt == "" || nmID == 0 {
			break
		}
		log.Printf("Загружено %d карточек, продолжаем...", len(allCards))
	}
	return allCards
}

type Card struct {
	NmID       int           `json:"nmID"`
	VendorCode string        `json:"vendorCode"`
	UpdatedAt  string        `json:"updatedAt"`
	Sizes      []ProductSize `json:"sizes"`
}

type ProductSize struct {
	SKUs []string `json:"skus"`
}

type CardsListResponse struct {
	Cards  []Card `json:"cards"`
	Cursor struct {
		UpdatedAt string `json:"updatedAt"`
		NmID      int    `json:"nmID"`
		Total     int    `json:"total"`
	} `json:"cursor"`
}

func extractSKUs(cards []Card) map[int][]string {
	skuMap := make(map[int][]string)
	for _, card := range cards {
		var skus []string
		for _, size := range card.Sizes {
			skus = append(skus, size.SKUs...)
		}
		skuMap[card.NmID] = skus
	}
	return skuMap
}

// func scrapeProductData(ctx context.Context, url string) (map[string]string, error) {
// 1) Нужно учесть, что в CSV файле у вас указано "bubblebags_19336", а в карточке приходит "bubblebags_19336_100".
//    То есть в CSV нет точного совпадения по ключу (vendorCode).
//    Нам нужно отбросить последний "_число", чтобы искать по "bubblebags_19336", а не "bubblebags_19336_100".
// 2) В scrapeProductData, когда паттерн совпал с ^bubblebags_1\d+_\d+$,
//    замените поиск csvURL, ок := bubblebagsURLMap[vendorCode] на поиск по "префиксу без третьей части":

func scrapeProductData(ctx context.Context, vendorCode string) (map[string]string, error) {
	// Проверяем: ^bubblebags_1\d+_\d+$
	matched, _ := regexp.MatchString(`^bubblebags_1\d+_\d+$`, vendorCode)
	if matched {
		// Пример: "bubblebags_19336_100"
		// Нам нужно отбросить "_100", чтобы найти "bubblebags_19336" в CSV
		baseKey := vendorCode
		if idx := strings.LastIndex(baseKey, "_"); idx != -1 {
			// baseKey = "bubblebags_19336"
			baseKey = baseKey[:idx]
		}

		// Ищем URL в карте, загруженной из CSV
		csvURL, ok := bubblebagsURLMap[baseKey]
		if !ok {
			log.Printf("Не найден URL для %s в urls.csv", vendorCode)
			return map[string]string{"price": "0", "availableCount": "0"}, nil
		}

		// Делаем chromedp-скрапинг по csvURL
		var htmlPrice, htmlStock string
		err := chromedp.Run(ctx,
			chromedp.Navigate(csvURL),
			chromedp.Sleep(2*time.Second),
			// Ищем наличие товара в <span class="stock">В наличии</span>
			chromedp.Text(`div.quantity span.stock`, &htmlStock, chromedp.ByQuery),
			// Ищем цену из кнопки data-count="1"
			chromedp.Text(`button[data-count="1"] .col_right`, &htmlPrice, chromedp.ByQuery),
		)
		if err != nil {
			return nil, fmt.Errorf("ошибка при парсинге страницы %s: %v", csvURL, err)
		}

		// Проверяем наличие
		var availableCount int
		if strings.Contains(htmlStock, "В наличии") {
			availableCount = 5
		} else {
			availableCount = 0
		}

		// Извлекаем число из htmlPrice (например, "23 руб.")
		priceParts := strings.Fields(htmlPrice)
		if len(priceParts) > 0 {
			rawPrice := priceParts[0]
			rawPrice = strings.ReplaceAll(rawPrice, "№", "")
			rawPrice = strings.TrimSpace(rawPrice)
			return map[string]string{
				"price":          rawPrice,
				"availableCount": fmt.Sprintf("%d", availableCount),
			}, nil
		}

		return map[string]string{"price": "0", "availableCount": fmt.Sprintf("%d", availableCount)}, nil
	}

	// Остальной код для "box_\d+_\d+$" и т. д.
	// (пример парсинга sp.cargo-avto.ru)
	parts := strings.Split(vendorCode, "_")
	if len(parts) < 2 {
		return nil, fmt.Errorf("некорректный VendorCode: %s", vendorCode)
	}
	url := baseURL + parts[1] + "/"

	var productPrice string
	var availableStoresCount int

	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.Sleep(2*time.Second),
		chromedp.Click(`li.tabs-item a[href="#samovivoz-tabs"]`, chromedp.ByQuery),
		chromedp.Sleep(2*time.Second),
		chromedp.Text(`li[data-min="1"] .price-val`, &productPrice, chromedp.ByQuery),
		chromedp.Evaluate(`document.querySelectorAll('.avail-item-status.avail').length`, &availableStoresCount),
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга страницы %s: %w", url, err)
	}

	productPrice = strings.TrimSpace(productPrice)
	productPrice = strings.ReplaceAll(productPrice, "p", "")
	productPrice = strings.ReplaceAll(productPrice, " ", "")

	return map[string]string{
		"price":          productPrice,
		"availableCount": fmt.Sprintf("%d", availableStoresCount),
	}, nil
}

func convertAndMultiply(priceStr, multiplierStr string) (int, error) {
	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return 0, fmt.Errorf("ошибка преобразования price: %v", err)
	}
	roundedPrice := int(math.Ceil(price))

	multiplier, err := strconv.Atoi(multiplierStr)
	if err != nil {
		return 0, fmt.Errorf("ошибка преобразования multiplier: %v", err)
	}
	return roundedPrice * multiplier, nil
}

func getCardsList(apiKey string, updatedAt string, nmID int, objectIDs []int) (*CardsListResponse, error) {
	url := "https://content-api.wildberries.ru/content/v2/get/cards/list"
	client := &http.Client{Timeout: 10 * time.Second}

	bodyData := map[string]interface{}{
		"settings": map[string]interface{}{
			"cursor": map[string]interface{}{
				"limit": 100,
			},
			"filter": map[string]interface{}{
				"withPhoto": 1,
				"objectIDs": objectIDs,
			},
		},
	}

	if updatedAt != "" {
		bodyData["settings"].(map[string]interface{})["cursor"].(map[string]interface{})["updatedAt"] = updatedAt
	}
	if nmID != 0 {
		bodyData["settings"].(map[string]interface{})["cursor"].(map[string]interface{})["nmID"] = nmID
	}

	bodyJSON, err := json.Marshal(bodyData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response CardsListResponse
	if err := json.Unmarshal(b, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

type SaveParams struct {
	NmID       int
	VendorCode string

	Pcs       int
	ProductID string

	AvailableCountStr string
	Cost              int
}

func saveToDatabase(db *sql.DB, params SaveParams, sku string) {
	availableCount, err := strconv.Atoi(params.AvailableCountStr)
	if err != nil {
		log.Printf("Ошибка при конвертации availableCount для %s: %v", params.ProductID, err)
		availableCount = 0
	}

	fmt.Printf("saveToDatabase: nmID: %d, vendorCode: %s, pcs: %d, productID: %s, sku: %s, availableCount: %d, cost: %d\n",
		params.NmID, params.VendorCode, params.Pcs, params.ProductID, sku, availableCount, params.Cost)

	query := `
			INSERT INTO products (
			nm_id, vendor_code,	pcs, product_id,sku, available_count, cost)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(product_id, pcs) DO UPDATE SET
			nm_id = excluded.nm_id,
			vendor_code = excluded.vendor_code,
			pcs = excluded.pcs,
			product_id = excluded.product_id,
			sku = excluded.sku,
			available_count = excluded.available_count,
			cost = excluded.cost;
		`

	_, err = db.Exec(query,
		params.NmID, params.VendorCode,
		params.Pcs, params.ProductID, sku,
		availableCount, params.Cost,
	)
	if err != nil {
		log.Printf("Ошибка при сохранении данных для %s: %v", params.ProductID, err)
	} else {
		log.Printf("Данные для товара %s успешно сохранены. SKUs: %s", params.ProductID, sku)
	}
}

func calcAmount(pcs, availableCount int) int {
	if availableCount == 5 && pcs == 50 {
		return 1
	} else if availableCount == 5 && pcs == 30 {
		return 2
	} else if availableCount == 5 && pcs == 10 {
		return 5
	} else if availableCount == 4 && pcs == 30 {
		return 1
	} else if availableCount == 4 && pcs == 10 {
		return 3
	}
	return 0
}

type stockItem struct {
	SKU    string `json:"sku"`
	Amount int    `json:"amount"`
}

// Структура для JSON, который отправляется в WB API
type stockRequest struct {
	Stocks []stockItem `json:"stocks"`
}
