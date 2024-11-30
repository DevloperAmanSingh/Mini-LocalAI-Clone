package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "file-processor",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{"code-bundles"}, nil)

	log.Println("Waiting for messages...")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			filePath := string(msg.Value)
			log.Printf("Received file path: %s", filePath)
			processFile(filePath)
		} else {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func processFile(filePath string) {
	log.Printf("Starting to process file: %s", filePath)

	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read file: %v", err)
		return
	}

	modelName := os.Getenv("MODEL_NAME")
	if modelName == "" {
		log.Println("MODEL_NAME environment variable is not set. Using default model.")
		modelName = "qwen2.5-coder:1.5b" // Default model
	}

	log.Printf("Processing output with model: %s", modelName)

	cmd := exec.Command("./GO-Native-LLM", "--model="+modelName, "--prompt=Generate the code review for the file and tell the bugs. Don't write full code. Can give snippets. Dont do formatiing.")
	cmd.Stdin = bytes.NewReader(content)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to execute command: %v", err)
		return
	}

	dir, file := filepath.Split(filePath)
	newFileName := strings.TrimSuffix(file, filepath.Ext(file)) + "_review" + filepath.Ext(file)
	newFilePath := filepath.Join(dir, newFileName)

	log.Printf("Writing processed output to new file: %s", newFilePath)

	err = ioutil.WriteFile(newFilePath, output, 0644)
	if err != nil {
		log.Printf("Failed to write output to new file: %v", err)
		return
	}

	log.Printf("Successfully processed and created new file: %s", newFilePath)
}
