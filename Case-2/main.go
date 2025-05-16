package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type ReconcileRequest struct {
	TransactionsPath   string   `json:"transactions_path"`
	BankStatementsPath []string `json:"bank_statements_path"`
	StartDate          string   `json:"start_date"`
	EndDate            string   `json:"end_date"`
}

type ReconcileResponse struct {
	TotalTransactionsProceed   int                            `json:"total_transactions_proceed"`
	TotalMatchedTransactions   int                            `json:"total_matched_transactions"`
	TotalUnmatchedTransactions TotalUnmatchedTransactionsData `json:"total_unmatched_transactions"`
}

type TotalUnmatchedTransactionsData struct {
	MissingTransactions   []Row            `json:"missing_transactions"`
	MissingBankStatements map[string][]Row `json:"missing_bank_statements"`
}

type TransactionsReadResponse struct {
	Rows                 [][]string
	MapRows              map[int]bool
	TransactionCounter   int
	amount               int
	UnmatchedTransaction map[int]Row
}

type ChannelResponse struct {
	result [][]string
	err    error
	amount int
}

type BankStatementPerRowResponse struct {
	Index  int
	Bank   string
	Row    []string
	Amount int
}

type Row struct {
	TransactionRowIndex int    `json:"key"`
	TrxID               string `json:"trx_id"`
	Amount              int    `json:"amount"`
	Type                string `json:"type"`
	TransactionTime     string `json:"transaction_time"`
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", reconcile).Methods(http.MethodPost)
	err := http.ListenAndServe(":3001", r)
	if err != nil {
		log.Fatal(err)
	}
}

func reconcile(w http.ResponseWriter, req *http.Request) {
	var bankStatementsResult [][]string
	_ = bankStatementsResult
	var validTransactionsKey []int
	var requestBody ReconcileRequest
	transactionsResult := TransactionsReadResponse{
		MapRows:              make(map[int]bool),
		UnmatchedTransaction: make(map[int]Row),
	}
	unbufferedCh := make(chan BankStatementPerRowResponse)
	unmatchedBankStatement := make(map[string]map[int]Row)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// ===== Read Request Body =====
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	err = json.Unmarshal(body, &requestBody)
	if err != nil {
		http.Error(w, "Failed to Unmarshally", http.StatusBadRequest)
		return
	}

	startDate, err := time.Parse("02-01-2006", requestBody.StartDate)
	if err != nil {
		http.Error(w, "Failed to parse start date", http.StatusBadRequest)
		return
	}

	endDate, err := time.Parse("02-01-2006", requestBody.EndDate)
	if err != nil {
		http.Error(w, "Failed to parse end date", http.StatusBadRequest)
		return
	}

	// ===== Reading CSV =====

	//	(Read Transactions)
	totalChannels := len(requestBody.BankStatementsPath)
	maxChannels := make(chan ChannelResponse, totalChannels)
	transactionsResult, err = readTransactions(requestBody.TransactionsPath, startDate, endDate)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	fmt.Println("Total transactions read:", transactionsResult.TransactionCounter)
	fmt.Println("Total amount read:", transactionsResult.amount)

	// V2 Read Bank Statements

	for _, v := range requestBody.BankStatementsPath {
		wg.Add(1)
		go readBankStatementsV2(ctx, maxChannels, unbufferedCh, &wg, v, startDate, endDate)
	}

	//go func() {
	//	wg.Wait()
	//	close(maxChannels)
	//}()

	go func() {
		for result := range unbufferedCh {
			fmt.Printf("[%d] Incoming Amount: %d", result.Amount, result.Amount)
			for k, item := range transactionsResult.Rows {
				amountVal, err := strconv.Atoi(item[1])
				if err != nil {
					http.Error(w, "Failed to parse amount", http.StatusBadRequest)
					return
				}

				transactionType := "KREDIT"
				if result.Amount < 1 {
					transactionType = "DEBIT"
				}

				// Only mark transaction that :
				// 1. Equal amount
				// 2. Equal type (DEBIT / KREDIT)
				// 3. never been inserted in validTransactionKey
				//fmt.Println("Compare")
				rowAmount := absInt(result.Amount)
				isValid := isTransactionValid(validTransactionsKey, amountVal)
				if amountVal == rowAmount && transactionType == result.Row[2] && !isValid {
					validTransactionsKey = append(validTransactionsKey, k)
					delete(transactionsResult.UnmatchedTransaction, k)
					delete(unmatchedBankStatement[result.Bank], k)
					break
				}

				if _, ok := unmatchedBankStatement[result.Bank]; !ok {
					unmatchedBankStatement[result.Bank] = make(map[int]Row)
				}

				// Append unmatched bank transaction
				unmatchedBankStatement[result.Bank][result.Index] = Row{
					TransactionRowIndex: k,
					TrxID:               result.Row[0],
					Amount:              amountVal,
					Type:                result.Row[1],
					TransactionTime:     result.Row[2],
				}

			}
		}
	}()

	fmt.Println("Stop At Waiting")
	wg.Wait()
	close(maxChannels)
	fmt.Println("finish Waiting")

	//for _, v := range transactionsResult.Rows {
	//	IsExistInArray()
	//}

	var response ReconcileResponse

	var missingTransactions []Row
	for _, v := range transactionsResult.UnmatchedTransaction {
		missingTransactions = append(missingTransactions, v)
	}

	missingBankStatements := make(map[string][]Row)
	for _, v := range requestBody.BankStatementsPath {
		for _, x := range unmatchedBankStatement[v] {
			missingBankStatements[v] = append(missingBankStatements[v], x)
		}
	}

	response.TotalTransactionsProceed = transactionsResult.TransactionCounter
	response.TotalMatchedTransactions = transactionsResult.TransactionCounter - len(transactionsResult.UnmatchedTransaction)
	response.TotalUnmatchedTransactions = TotalUnmatchedTransactionsData{
		MissingTransactions:   missingTransactions,
		MissingBankStatements: missingBankStatements,
	}

	//fmt.Println("Total Transaction Processed: ", transactionsResult.TransactionCounter)
	//fmt.Println("Total Matched Transaction: ", transactionsResult.TransactionCounter-len(transactionsResult.UnmatchedTransaction))
	//fmt.Println("Total Unmatched Transaction: ", len(transactionsResult.UnmatchedTransaction))
	//fmt.Println("List Of Missing Transactions in Bank Statements ")
	//for _, v := range transactionsResult.UnmatchedTransaction {
	//	fmt.Println(v)
	//}
	//
	//fmt.Println("List Of Missing Bank Statements In Transactions ")
	//for k, itemMatchedBankTransaction := range unmatchedBankStatement {
	//	fmt.Println("Bank Path: ", k)
	//	for _, v := range itemMatchedBankTransaction {
	//
	//	}
	//}

	payload, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Failed marshall", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(payload)
}

func readBankStatementsV2(ctx context.Context, ch chan ChannelResponse, unbufferedCh chan BankStatementPerRowResponse, wg *sync.WaitGroup, path string, startDate, endDate time.Time) {
	defer wg.Done()
	response := ChannelResponse{}
	var transactionRowIndex int

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context cancelled")
			return
		default:
			bankStatementFile, err := os.Open(path)
			if err != nil {
				fmt.Println(err)
				response.err = errors.New("Failed to open transcations path")
				ch <- response
				return
			}

			defer bankStatementFile.Close()
			bankStatementReader := csv.NewReader(bankStatementFile)
			_, err = bankStatementReader.Read()
			if err != nil {
				err = errors.New("Error reading header row")
			}

			for {
				record, err := bankStatementReader.Read()
				if err == io.EOF {
					break
				}

				if err != nil {
					fmt.Println(err)
					response.err = errors.New("Failed to read transcations")
				}

				transactionDate, err := time.Parse(time.RFC3339, record[3])
				if err != nil {
					fmt.Println(err)
				}

				isInDateRange := startDate.Before(transactionDate.UTC()) && endDate.After(transactionDate.UTC())
				if !isInDateRange {
					continue
				}

				amountVal, err := strconv.Atoi(record[1])
				if err != nil {
					fmt.Println(err)
				}
				response.amount = response.amount + amountVal
				response.result = append(response.result, record)

				fmt.Println("Send Amount:", response.amount, " From Path: ", path)
				unbufferedCh <- BankStatementPerRowResponse{
					Index:  transactionRowIndex,
					Row:    record,
					Amount: amountVal,
					Bank:   path,
				}
				transactionRowIndex++
			}

			fmt.Println("Final Result Path: ", path, " Amount: ", response.amount)
			ch <- response
			return
		}
	}
}

func readTransactions(path string, startDate, endDate time.Time) (TransactionsReadResponse, error) {
	response := TransactionsReadResponse{
		UnmatchedTransaction: make(map[int]Row),
		MapRows:              make(map[int]bool),
	}

	transactionFile, err := os.Open(path)
	if err != nil {
		err = errors.New("Failed to open transactions path")
		fmt.Println(err)
		return TransactionsReadResponse{}, err
	}

	defer transactionFile.Close()
	transactionsReader := csv.NewReader(transactionFile)

	_, err = transactionsReader.Read()
	if err != nil {
		err = errors.New("Error reading header row")
		return TransactionsReadResponse{}, err
	}

	for {
		record, err := transactionsReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			return TransactionsReadResponse{}, err
		}

		transactionDate, err := time.Parse(time.RFC3339, record[3])
		if err != nil {
			fmt.Println(err)
			return TransactionsReadResponse{}, err
		}

		isInDateRange := startDate.Before(transactionDate.UTC()) && endDate.After(transactionDate.UTC())
		if !isInDateRange {
			continue
		}

		amountVal, err := strconv.Atoi(record[1])
		if err != nil {
			fmt.Println(err)
			return TransactionsReadResponse{}, err
		}
		response.amount = response.amount + amountVal

		response.Rows = append(response.Rows, record)
		response.UnmatchedTransaction[response.TransactionCounter] = Row{
			TransactionRowIndex: response.TransactionCounter,
			TrxID:               record[0],
			Amount:              amountVal,
			Type:                record[1],
			TransactionTime:     record[2],
		}
		response.MapRows[response.TransactionCounter] = true

		response.TransactionCounter++
	}
	return response, nil
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func isTransactionValid(list []int, val int) (isValid bool) {
	for _, v := range list {
		if v == val {
			isValid = true
			break
		}
	}
	return
}

func IsExistInArray(list []string, indexKey int) bool {
	var isExist bool
	for k, _ := range list {
		if k == indexKey {
			isExist = true
			break
		}
	}
	return isExist
}
