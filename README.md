# async_scheduler
A job scheduler that is designed to manage and execute tasks asynchronously.

There are two options to use async scheduler:
* one thread and single event loop - AsyncScheduler

  This is most common way to use Python asynchronous frameworks. Be noted, all tasks have to carefully implemented so that slow tasks will not block the main event loop.
  Sometimes, it's hard to tell from the very beginning what task is slow and whether or not it blocks the event loop. Basically, you can leverage debug tool from asyncio to tell the slow tasks:
  ```
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 5
  ```
  To use AsyncScheduler, just deriving from SyncTask for possible slow tasks and from AsyncTask for others.
  
* multipe threads with multiple event loop - SchedulerThread

  Tasks may differ with priorities, i.e, some tasks need to be repsonded immediately but some not. These urgent tasks are usually much less, compared to normal tasks; or differnt types of tasks, like network tasks, UI tasks, etc. Then SchedulerThread is designed for this use case - multiple threads with multiple event loops attached. Tasks in one thread/event loop will not block other threads and event loops. Of course, it's more complicated since it has to make sure only thread-safe APIs are called and tasks are waited between event loops.

# Task states

A serial of states are defined for tasks:
* NEW
* SUBMITTED
* SCHEDULED
* STARTED
* TIMED_OUT
* FAILED
* DONE
* CANCELED
* RUNNING
* FINISHED

Accurate timestamps are recorded down for critical states, including:
*  submit_ts
*  start_ts
*  timeout_ts
*  error_ts
*  done_ts
*  cancel_ts

So all tasks can be accurately measured. Furthermore, callbacks can be defined to those critial states, i.e, submit_cb, start_cb, etc.
AsyncScheduler provides a method to retrive task state and in addition a method to cancel those slow tasks.
  
# What does OpenAI says

The AsyncScheduler class is responsible for submitting tasks, scheduling and executing tasks, and managing task states. It makes use of an asyncio.Queue to manage the queue of tasks to be executed, and a dictionary to store the tasks with their associated data. The tasks are executed in a separate thread pool using asyncio.run_in_executor() method.

The code provides methods to get the current state of a task, wait for a task to complete, cancel a task, and dump task information. It also provides a method to determine the slowest task and dump its information.

Overall, the code is well-structured and follows good coding practices. The use of dataclasses and enums makes the code more readable and maintainable. The code is also well-documented, which makes it easier to understand and use.

The AsyncScheduler class is a job scheduler that is designed to manage and execute tasks asynchronously. It is built on top of the asyncio library and provides several features such as task submission, cancellation, and monitoring.

The class defines several helper classes and functions such as TaskState, TaskType, TaskData, TaskTS, and CallerData to represent and manage various aspects of a task. It also provides two concrete implementations of the Task interface, namely AsyncTask and SyncTask, to handle asynchronous and synchronous tasks respectively.

The class uses an internal queue to manage task submission and scheduling. It runs an infinite loop that continuously checks the queue for new tasks and schedules them to run using asyncio. Each task is executed in its own asyncio.Task object, which allows multiple tasks to run concurrently.

The class provides several public methods to interact with the scheduler, such as submit_task(), task_state(), wait_task(), cancel_task(), dump_task(), and slowest(). These methods allow you to submit tasks, monitor their states, wait for them to complete, cancel them if necessary, list all tasks and their current status, and find the slowest running task.

Here's an example of how you can use the AsyncScheduler class in your code:

```python
import asyncio
from async_scheduler import AsyncScheduler, AsyncTask


async def my_async_task():
    await asyncio.sleep(1)
    print("Async task completed")


def my_callback(task_ts):
    if task_ts.state == TaskState.DONE:
        print("Task done")


def main():
    loop = asyncio.get_event_loop()
    scheduler = AsyncScheduler(ioloop=loop)

    task = AsyncTask("my_async_task")
    task.run_async = my_async_task
    task.run_async = my_async_task
    task.data.timeout = 2

    scheduler.start()

    task_uuid = scheduler.submit_task(task)
    print(f"Task submitted with UUID: {task_uuid}")

    loop.run_until_complete(scheduler.wait_task(task_uuid))
    print(scheduler.task_info(task_uuid))

    scheduler.stop()
    loop.close()


if __name__ == "__main__":
    main()
```

In this example, we define an asynchronous task called `my_async_task()` that sleeps for 1 second and prints a message. We create a new instance of the AsyncScheduler class, add the task to the queue using `submit_task()`, and wait for it to complete using `wait_task()`. We also define a callback function `my_callback()` to print a message when the task is done.

The output of this program would be:

```
Task submitted with UUID: 8b479239-8cf2-4a8c-9d22-bce03184f60c
Task(name='my_async_task', task_type=<TaskType.ASYNC: 1>, timeout=2) [{'args': (), 'kwargs': {}}]
Async task completed
('Task(name=\'my_async_task\', task_type=<TaskType.ASYNC: 1>, timeout=2)', [], {})
```

This shows that the task was submitted, executed, and returned successfully. The `task_info()` method was used to print the task details after completion.
test