package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	db          *sql.DB
)

type Product struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
}

func connectRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.Background()
	fmt.Print(client.Ping(ctx).Result())
	return client, nil
}

func connectDB() (*sql.DB, error) {
	db, err := sql.Open("mysql", "root:dream@tcp(127.0.0.1:3306)/gogo")
	if err != nil {
		return nil, err
	}
	return db, nil
}

// retrieves product from Redis
func GetProductHandler(w http.ResponseWriter, r *http.Request) {
	//get id from request
	params := mux.Vars(r)
	id := params["id"]

	//check if product exists
	productJSON, err := redisClient.Get(context.Background(), id).Result()
	if err == nil {
		//found in Redis cache, return cached data
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(productJSON))
		fmt.Println("Taken from Redis")
		return
	}

	product, err := getProductFromDB(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	//store product
	productJSON1, _ := json.Marshal(product)
	err = redisClient.Set(context.Background(), id, productJSON1, 10*time.Minute).Err()
	if err != nil {
		log.Println("Failed to cache product:", err)
	}
	fmt.Println("Taken from DB")

	//return product
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(product)
}

// retrieves a product from db
func getProductFromDB(id string) (*Product, error) {
	var product Product
	query := "SELECT id, name, description, price FROM products WHERE id = ?"

	row := db.QueryRow(query, id)
	err := row.Scan(&product.ID, &product.Name, &product.Description, &product.Price)
	if err != nil {
		return nil, err
	}

	return &product, nil
}

func main() {
	//connect to Redis
	var err error
	redisClient, err = connectRedis()
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	defer redisClient.Close()

	//connect to database
	db, err = connectDB()
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	//initialize router
	router := mux.NewRouter()

	//define routes
	router.HandleFunc("/product/{id}", GetProductHandler).Methods("GET")

	//start server
	log.Println("Server started on port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}
