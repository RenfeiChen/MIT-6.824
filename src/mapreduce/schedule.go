package mapreduce

import (
	"fmt"
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

	// workChan to check if all the tasks have been done

	workChan := make(chan int)

	// Add all task numbers to taskNumber channel and use workChan to wait
	// all tasks to get completed, remmmember to close the channel or we cannot
	// use the range since it wll get dead lock, the receiver doesn't know if
	// it has ended the reason why we use a new goroutine is that we can make
	// sure when we insert a task number into the channel there will be a
	// receriver to receive this task concurrently

	// IMPORTANT!!
	// We can't put the workChan outside of this goroutine like putting it in
	// the main function, since if we do that, when we have finished the for
	// loop of the taskNumberChan, we will close the taskNumberChan, and if
	// there are some error works, and we will return them but it has been
	// closed, it wll get error

	taskNumberChan := make(chan int)
	go func() {
		for i := 0; i < ntasks; i++ {
			taskNumberChan <- i
		}
		// wait all the tasks to be done
		for i := 0; i < ntasks; i++ {
			<-workChan
		}
		// when all tasks have been done, we can close the taskNumberChan
		close(taskNumberChan)
	}()

	// Schedule all tasks to works

	for i := range taskNumberChan {
		// create a goroutine to handle the i task
		go func(jobName string, i int, n_other int, phase jobPhase) {
			task := DoTaskArgs{JobName: jobName, TaskNumber: i, NumOtherPhase: n_other, Phase: phase}
			if phase == mapPhase {
				task.File = mapFiles[i]
			}
			// get the free worker
			worker := <-registerChan
			// schedule the task to the worker
			if call(worker, "Worker.DoTask", task, nil) {
				// IMPORTANT!!
				// We need to create a goroutine to return the work to the
				// registerChan, since it is unbuffered so when there are some
				// other free works, it will block this function, so we need to
				// create a new goroutine to wait the registerChan
				go func() {
					registerChan <- worker
				}()
				// send 1 to the channel means we have already done a task
				workChan <- 1
			} else {
				// the worker is not available we need to reschdule this task
				// number so we need to return the number to the taskNumberChan
				taskNumberChan <- task.TaskNumber
			}
		}(jobName, i, n_other, phase)
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
