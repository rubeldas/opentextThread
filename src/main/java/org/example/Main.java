package org.example;

/*
Implement a task executor service according to the following specification.
The entry point for the service is Task Executor interface. The interface is defined bellow including its dependencies.
The service is required to implement the following behaviors:
    1.	Tasks can be submitted concurrently. Task submission should not block the submitter.
    2.	Tasks are executed asynchronously and concurrently. Maximum allowed concurrency may be restricted.
    3.	Once task is finished, its results can be retrieved from the Future received during task submission.
    4.	The order of tasks must be preserved.
        o	The first task submitted must be the first task started.
        o	The task result should be available as soon as possible after the task completes.
    5.	Tasks sharing the same TaskGroup must not run concurrently.

     Additional implementation requirements:
            1.	The implementation must run on OpenJDK 17.
            2.	No third-party libraries can be used.
            3.	The provided interfaces and classes must not be modified.
                    Please, write down any assumptions you made.
*/


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Function;

public class Main {


    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /** 
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }


    public static class TaskExecutorExample implements  TaskExecutor{

        private final int maxConcurrency;
        private final ExecutorService executorService;
        private final LinkedBlockingQueue<Runnable> taskQueue;
        private final ConcurrentHashMap<TaskGroup, Semaphore> taskGroupVsSemaphore;
        private volatile boolean flag = true;

        TaskExecutorExample(int maxConcurrency){
            this.maxConcurrency = maxConcurrency;
            executorService = Executors.newFixedThreadPool(maxConcurrency);
            taskQueue = new LinkedBlockingQueue<>();
            taskGroupVsSemaphore = new ConcurrentHashMap<>();
            activateThreadPool();
        }

        private void activateThreadPool() {

            for(int i = 0 ; i < maxConcurrency ; i++ ) {
                // Each thread will keep on running till its not stopped.
                executorService.submit(() -> {
                    while(flag){
                        try {
                            // Getting task from queue and executing it.
                            Runnable runnable = taskQueue.take();
                            runnable.run();
                        } catch(Exception e){
                            System.out.println("InterruptedException ... ");
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                });
                System.out.println("Finished  : " + i);
            }
        }

        public void shutdown(){
            taskQueue.clear();
            flag = false;
            executorService.shutdown();
            try {
                // Wait for currently executing tasks to finish
                if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    executorService.shutdownNow(); // Force shutdown if tasks don't finish in time
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow(); // Re-interrupt the thread if it was interrupted during shutdown
                Thread.currentThread().interrupt();
            }

        }

        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        @Override
        public <T> Future<T> submitTask(Task<T> task){
            CompletableFuture future = new CompletableFuture();
            // Submitting tasks by offer method. If full it returns false.
            taskQueue.offer(()->{
                try {
                    taskGroupVsSemaphore.computeIfAbsent(task.taskGroup(), k -> new Semaphore(1)).acquire();
                    System.out.println(" Executing " + Thread.currentThread() + " Task : " + task);
                    String str = (String) task.taskAction().call();
                    future.complete(str);
                }catch (Exception e) {
                    System.out.println(" Under Exception " + Thread.currentThread());
                    future.completeExceptionally(e);
                } finally {
                    System.out.println(" Releasing lock for  group : " + task.taskGroup());
                    taskGroupVsSemaphore.get(task.taskGroup()).release();
                }
            });
            return future;
        }
    }



    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID Unique task identifier.
     * @param taskGroup Task group.
     * @param taskType Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T> Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction) {

        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }

    }




    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID
    ) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }


    public static void main(String args[]) throws InterruptedException, ExecutionException {
        TaskExecutorExample taskExecutorExample = new TaskExecutorExample(3);

        CompletableFuture f1 = (CompletableFuture) taskExecutorExample.submitTask(new Task<>(UUID.randomUUID(), new TaskGroup(UUID.randomUUID()),
                TaskType.READ, new Callable<>() {
            @Override
            public String call() throws Exception {
                return "Callable1 completed";
            }
        }
        ));

        CompletableFuture f2 = (CompletableFuture) taskExecutorExample.submitTask(new Task<>(UUID.randomUUID(), new TaskGroup(UUID.randomUUID()),
                TaskType.READ, new Callable<>() {
            @Override
            public Object call() throws Exception {
                return "Callable2 completed";
            }
        }
        ));

        // Same group :

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());

        CompletableFuture f3 = (CompletableFuture) taskExecutorExample.submitTask(new Task<>(UUID.randomUUID(), group1,
                TaskType.WRITE, new Callable<>() {
            @Override
            public Object call() throws Exception {
                return "Callable3 completed";
            }
        }
        ));

        CompletableFuture f4 = (CompletableFuture) taskExecutorExample.submitTask(new Task<>(UUID.randomUUID(), group1,
                TaskType.WRITE, new Callable<>() {
            @Override
            public Object call() throws Exception {
                return "Callable4 completed";
            }
        }
        ));

        try {
              System.out.println("Future 1 : " + f1.get());
              System.out.println("Future 2 : " + f2.get());
              System.out.println("Future 3 : " + f3.get());
              System.out.println("Future 4 : " + f4.get());
        }catch (Exception e){
            e.printStackTrace();
        }
        taskExecutorExample.shutdown();
        System.out.println("End of main.");


    }


}
