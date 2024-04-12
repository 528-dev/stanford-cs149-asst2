#include <thread>
#include <mutex>
#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), max_num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::mutex* mtx = new std::mutex();
    std::vector<std::thread> vt(max_num_threads);
    int cnt = 0;

    auto func = [&cnt, mtx, num_total_tasks, runnable] () {
        while (true) {
            mtx->lock();
            int tmp = cnt ++;
            mtx->unlock();

            if (tmp >= num_total_tasks) return;

            runnable->runTask(tmp, num_total_tasks);

        }
    };

    for (int i = 1; i < max_num_threads; ++ i) {
        vt[i] = std::thread(func);
    }
    func();

    for (int i = 1; i < max_num_threads; ++i) {
        vt[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int m_num_threads): 
    ITaskSystem(m_num_threads), 
    num_threads(m_num_threads), 
    vt(m_num_threads),
    kill_all(false), 
    mtx_cnt_finished(new std::mutex()), 
    mtx(new std::mutex()),
    runnable(nullptr),
    num_total_tasks(0),
    cnt_finished(0),
    cur_task_id(0),
    cv(new std::condition_variable())
    {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    auto func = [this] () {
        while (!kill_all) {

            // get the id of task to be done; 
            mtx->lock();
            int id = cur_task_id;
            if (id >= num_total_tasks) {
                mtx->unlock();
                continue;
            }
            if (cur_task_id < num_total_tasks) cur_task_id ++;
            mtx->unlock();

            // go go 
            runnable->runTask(id, num_total_tasks);

            // whether finished all tasks ?
            mtx_cnt_finished->lock();
            cnt_finished ++;
            mtx_cnt_finished->unlock();
            
            if (cnt_finished == num_total_tasks) cv->notify_one();

        }
    };

    for (int i = 0; i < num_threads; ++ i) {
        vt[i] = std::thread(func);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    kill_all = true;
    for (int i = 0; i < num_threads; ++ i) {
        vt[i].join();
    }
}

#include <unistd.h>
void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> lk(*mtx_cnt_finished);

    mtx->lock();
    this->cur_task_id = 0;
    this->num_total_tasks = num_total_tasks;
    this->runnable = runnable;
    this->cnt_finished = 0;
    mtx->unlock();

    cv->wait(lk, [&]{return cnt_finished == num_total_tasks;});
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int m_num_threads): 
    ITaskSystem(m_num_threads), 
    num_threads_(m_num_threads), 
    vt_(m_num_threads),
    kill_all_(false),

    cur_taskid_(0),
    num_total_tasks_(0),
    num_finished_(0)

    {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    auto func = [&] () {
        while (!kill_all_) {
            std::unique_lock<std::mutex> lk(mtx);
            cv1.wait(lk, [this]{return kill_all_ || cur_taskid_ < num_total_tasks_;});
            int id = cur_taskid_ ++; // mutex access shared variable.
            lk.unlock();

            if (kill_all_) return;
            runnable_->runTask(id, num_total_tasks_);

            mtx2.lock();
            num_finished_ ++;
            mtx2.unlock();
            // if (id == num_total_tasks_ - 1) cv2.notify_one(); // dont work

            if ( num_finished_ == num_total_tasks_) cv2.notify_one();
        }
    };

    for (int i = 0; i < num_threads_; ++ i) {
        vt_[i] = std::thread(func);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    kill_all_ = true;
    cv1.notify_all();
    for (int i = 0; i < num_threads_; ++ i) {
        vt_[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // initailize
    {
        std::lock_guard<std::mutex> lk(mtx);
        runnable_ = runnable;
        num_total_tasks_ = num_total_tasks;
        num_finished_ = 0;
        cur_taskid_ = 0;
    }
    cv1.notify_all();
    std::unique_lock<std::mutex> lk(mtx2);
    cv2.wait(lk, [&]{return num_finished_ == num_total_tasks_;});
    // cv2.wait(lk);
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
