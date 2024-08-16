package smrecover

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
)

type FAILED_STATUS types.ExecutionStatus

var (
	FAILED    FAILED_STATUS = "FAILED"
	TIMED_OUT FAILED_STATUS = "TIMED_OUT"
)

type SmRecover struct {
	client            *sfn.Client
	stateMachineArn   string
	fromUnixTimestamp int64
	toUnixTimestamp   int64
}

func NewSmRecover(stateMachineArn string) *SmRecover {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	return &SmRecover{
		client:          sfn.NewFromConfig(cfg),
		stateMachineArn: stateMachineArn,
	}
}

func (sm *SmRecover) listExecutions(nextToken *string,
	statusFilter FAILED_STATUS) ([]types.ExecutionListItem, *string) {
	params := sfn.ListExecutionsInput{
		StateMachineArn: &sm.stateMachineArn,
		MaxResults:      1000,
	}
	if nextToken != nil && *nextToken != "" {
		params.NextToken = nextToken
	}
	if statusFilter == FAILED || statusFilter == TIMED_OUT {
		params.StatusFilter = types.ExecutionStatus(statusFilter)
	}
	ret, err := sm.client.ListExecutions(context.TODO(), &params)
	if err != nil {
		panic(err)
	}
	var filtered []types.ExecutionListItem
	if sm.fromUnixTimestamp == 0 && sm.toUnixTimestamp == 0 {
		filtered = ret.Executions
	} else {
		for _, exec := range ret.Executions {
			if (sm.fromUnixTimestamp == 0 || exec.StartDate.Unix() >= sm.fromUnixTimestamp) &&
				(sm.toUnixTimestamp == 0 || exec.StartDate.Unix() <= sm.toUnixTimestamp) {
				filtered = append(filtered, exec)
			}
		}
	}
	return filtered, ret.NextToken
}

func (sm *SmRecover) listAllExecutions(statusFilter FAILED_STATUS) <-chan []types.ExecutionListItem {
	c := make(chan []types.ExecutionListItem)
	go func() {
		var (
			execs     []types.ExecutionListItem
			nextToken *string
		)
		for i := 0; i == 0 || nextToken != nil; i++ {
			execs, nextToken = sm.listExecutions(nextToken, statusFilter)
			c <- execs
		}
	}()
	return c
}

func (sm *SmRecover) describeExecution(executionArn *string) *sfn.DescribeExecutionOutput {
	params := sfn.DescribeExecutionInput{
		ExecutionArn: executionArn,
	}
	ret, err := sm.client.DescribeExecution(context.TODO(), &params)
	if err != nil {
		panic(err)
	}
	return ret
}

func (sm *SmRecover) startExecution(jsonInput *string) *sfn.StartExecutionOutput {
	params := sfn.StartExecutionInput{
		StateMachineArn: &sm.stateMachineArn,
		Input:           jsonInput,
	}
	ret, err := sm.client.StartExecution(context.TODO(), &params)
	if err != nil {
		panic(err)
	}
	return ret
}

func (sm *SmRecover) restart(statusFilter FAILED_STATUS) {
	for execs := range sm.listAllExecutions(statusFilter) {
		for _, exec := range execs {
			desc := sm.describeExecution(exec.ExecutionArn)
			start := sm.startExecution(desc.Input)
			fmt.Printf("restarted execution: %s\nnew execution: %s\n\n",
				*exec.ExecutionArn, *start.ExecutionArn)
		}
	}
}

func (sm *SmRecover) redriveExecution(executionArn *string) *sfn.RedriveExecutionOutput {
	params := sfn.RedriveExecutionInput{
		ExecutionArn: executionArn,
	}
	ret, err := sm.client.RedriveExecution(context.TODO(), &params)
	if err != nil {
		panic(err)
	}
	return ret
}

func (sm *SmRecover) redrive(statusFilter FAILED_STATUS) {
	for execs := range sm.listAllExecutions(statusFilter) {
		for _, exec := range execs {
			sm.redriveExecution(exec.ExecutionArn)
			fmt.Printf("redriven execution: %s\n\n", *exec.ExecutionArn)
		}
	}
}

func (sm *SmRecover) RestartFailed() {
	sm.restart(FAILED)
}

func (sm *SmRecover) RestartTimedOut() {
	sm.restart(TIMED_OUT)
}

func (sm *SmRecover) RedriveFailed() {
	sm.redrive(FAILED)
}

func (sm *SmRecover) RedriveTimedOut() {
	sm.redrive(TIMED_OUT)
}

func (sm *SmRecover) SetTimestamps(fromUnixTimestamp, toUnixTimestamp int64) {
	sm.fromUnixTimestamp = fromUnixTimestamp
	sm.toUnixTimestamp = toUnixTimestamp
}
