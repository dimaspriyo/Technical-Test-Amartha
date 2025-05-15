package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"os"
)

type ReconcileRequest struct {
	TransactionsPath   string   `json:"transactions_path"`
	BankStatementsPath []string `json:"bank_statements_path"`
}

type ChannelResponse struct {
	result [][]string
	err    error
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", reconcile).Methods(http.MethodPost)
	http.ListenAndServe(":3000", r)
}

func reconcile(w http.ResponseWriter, req *http.Request) {
	var transactionsResult, bankStatementsResult [][]string
	_ = transactionsResult

	// ===== Read Request Body =====
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	var requestBody ReconcileRequest
	err = json.Unmarshal(body, &requestBody)
	if err != nil {
		http.Error(w, "Failed to Unmarshally", http.StatusBadRequest)
		return
	}

	// ===== Reading CSV =====
	totalChannels := len(requestBody.BankStatementsPath)
	maxChannels := make(chan ChannelResponse, totalChannels)

	transactionFile, err := os.Open(requestBody.TransactionsPath)
	if err != nil {
		fmt.Println(err)
		http.Error(w, "Failed to open transcations path", http.StatusInternalServerError)
		return
	}

	defer transactionFile.Close()
	transactionsReader := csv.NewReader(transactionFile)
	transactionCsvRead, err := transactionsReader.ReadAll()
	_ = transactionCsvRead
	if err != nil {
		fmt.Println(err)
		http.Error(w, "Failed to read transcations", http.StatusInternalServerError)
		return
	}

	for _, v := range requestBody.BankStatementsPath {
		go readCsv(maxChannels, v)
	}

	for i := 1; i <= totalChannels; i++ {
		result := <-maxChannels
		if result.err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		bankStatementsResult = append(bankStatementsResult, result.result...)
	}

	// ===== Start To Reconcile =====
	fmt.Println("HERE")
}

func readCsv(ch chan ChannelResponse, path string) {

	response := ChannelResponse{}

	transactionFile, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		response.err = errors.New("Failed to open transcations path")
		ch <- response
		return
	}

	defer transactionFile.Close()
	transactionsReader := csv.NewReader(transactionFile)
	transactionsRecords, err := transactionsReader.ReadAll()
	if err != nil {
		fmt.Println(err)
		response.err = errors.New("Failed to read transcations")
		ch <- response
		return
	}

	response.result = transactionsRecords[1:]
	ch <- response
	return
}
