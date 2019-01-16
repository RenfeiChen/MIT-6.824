package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup
	wg.Add(ntasks)

	// Add all task numbers to taskNumber channel and use waitgroup to wait
	// all tasks to get completed, remmmember to close the channel or we cannot
	// use the range since it wll get dead lock, the receiver doesn't know if
	// it has ended the reason why we use a new goroutine is that we can make
	// sure when we insert a task number into the channel there will be a
	// receriver to receive this task concurrently

	// ##Important
	// We need to use wg.Wait() earlier than closing the channel
	// Or we can use the wg.Wait() in the main function
	// The reason is that if we put close ealier than wait, after receiving all
	// taskNumber, the i loop in the main function will end, no matter if there
	// is any running goroutine, the main function will end, so the gorouinte
	// will get terminated, and the task cannot be done

	taskNumberChan := make(chan int)
	go func() {
		for i := 0; i < ntasks; i++ {
			taskNumberChan <- i
		}
		//wg.Wait()
		close(taskNumberChan)
	}()

	// Schedule all tasks to works

	for i := range taskNumberChan {
		// declare the taskArgs
		task := DoTaskArgs{JobName: jobName, TaskNumber: i, NumOtherPhase: n_other, Phase: phase}
		if phase == mapPhase {
			task.File = mapFiles[i]
		}
		// get the free worker
		worker := <-registerChan
		// schedule the task to the worker
		go func(task DoTaskArgs, worker string) {
			if call(worker, "Worker.DoTask", task, nil) {
				// if the worker is available we should let the wg minus 1 and
				// return the worker to the register channel
				wg.Done()
				registerChan <- worker
			} else {
				// the worker is not available we need to reschdule this task
				// number so we need to return the number to the taskNumberChan
				taskNumberChan <- task.TaskNumber
			}
		}(task, worker)
	}

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
