#!/usr/bin/env python

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
import uuid
from addict import Dict
import socket
import getpass
from threading import Thread
import signal
import time
import enum

MESOS_MASTER_IP = "139.59.11.188"

class TryTask:
    def __init__(self, taskId, command, cpu=.1, mem=10):
        self.taskId = taskId
        self.command = command
        self.cpu = cpu
        self.mem = mem

    def toString(self):
        str = "TaskId:%s, Cmd:%s, CPU=%d, MEM=%d" % (self.taskId, self.command, self.cpu, self.mem)
        return str

    def __getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def __updateResource(self, res, name, value):
        if value <= 0:
            return
        for r in res:
            if r.name == name:
                r.scalar.value -= value
        return

    def acceptOffer(self, offer):
        accept = True
        if self.cpu != 0:
            cpu = self.__getResource(offer.resources, "cpus")
            if self.cpu > cpu:
                accept =  False
        if self.mem != 0:
            mem = self.__getResource(offer.resources, "mem")
            if self.mem > mem:
                accept = False
        if(accept == True):
            self.__updateResource(offer.resources, "cpus", self.cpu)
            self.__updateResource(offer.resources, "mem", self.mem)
            return True
        else:
            return False

class TryScheduler(Scheduler):
    def __init__(self):
        self.idleTaskList = []
        self.startingTaskList = {}
        self.runningTaskList = {}
        self.terminatingTaskList = {}
        self.idleTaskList.append(TryTask("task1", "echo TryTask-task1 && sleep 500"))
        self.idleTaskList.append(TryTask("task2", "echo TryTask-task2 && sleep 500"))
        self.idleTaskList.append(TryTask("task3", "echo TryTask-task3 && sleep 500", .1, 100))
        self.idleTaskList.append(TryTask("task4", "echo TryTask-task4 && sleep 500", .1, 200))
        self.idleTaskList.append(TryTask("task5", "echo TryTask-task5 && sleep 500", .2, 200))

    def resourceOffers(self, driver, offers):
        logging.debug("Received new offers")

        if(len(self.idleTaskList) == 0):
            driver.suppressOffers()
            logging.info("Suppressing Offers")
            return

        filters = {'refuse_seconds': 1}

        for offer in offers:
            taskList = []
            pendingTaksList = []
            while True:
                if len(self.idleTaskList) == 0:
                    break
                TryTask = self.idleTaskList.pop(0)
                if TryTask.acceptOffer(offer):
                    task = Dict()
                    task_id = TryTask.taskId
                    task.task_id.value = task_id
                    task.agent_id.value = offer.agent_id.value
                    task.name = 'task {}'.format(task_id)
                    task.command.value = TryTask.command
                    task.resources = [
                        dict(name='cpus', type='SCALAR', scalar={'value': TryTask.cpu}),
                        dict(name='mem', type='SCALAR', scalar={'value': TryTask.mem}),
                    ]
                    self.startingTaskList[task_id] = TryTask
                    taskList.append(task)
                    logging.info("Starting task: %s, in node: %s" % (TryTask.taskId, offer.hostname))
                else:
                    pendingTaksList.append(TryTask)

            if(len(taskList)):
                    driver.launchTasks(offer.id, taskList, filters)

            self.idleTaskList = pendingTaksList

    def statusUpdate(self, driver, update):
        if update.state == "TASK_STARTING":
            TryTask = self.startingTaskList[update.task_id.value]
            logging.debug("Task %s is starting." % update.task_id.value)
        elif update.state == "TASK_RUNNING":
            if update.task_id.value in self.startingTaskList:
                TryTask = self.startingTaskList[update.task_id.value]
                logging.info("Task %s running in %s. Moving to running list" %
                (update.task_id.value, update.container_status.network_infos[0].ip_addresses[0].ip_address))
                self.runningTaskList[update.task_id.value] = TryTask
                del self.startingTaskList[update.task_id.value]
        elif update.state == "TASK_FAILED":
            TryTask = None
            if update.task_id.value in self.startingTaskList:
                TryTask = self.startingTaskList[update.task_id.value]
                del self.startingTaskList[update.task_id.value]
            elif update.task_id.value in self.runningTaskList:
                TryTask = self.runningTaskList[update.task_id.value]
                del self.runningTaskList[update.task_id.value]
            if TryTask:
                logging.info("Uni task: %s failed." % TryTask.taskId)
                self.idleTaskList.append(TryTask)
                driver.reviveOffers()
            else:
                logging.error("Received task failed for unknown task: %s" % update.task_id.value )
        else:
            logging.info("Received status %s for task id: %s" % (update.state, update.task_id.value))

def main():
    framework = Dict()
    framework.user = getpass.getuser()
    framework.id.value = str(uuid.uuid4())
    framework.name = "TRY"
    framework.hostname = socket.gethostname()
    framework.failover_timeout = 75

    driver = MesosSchedulerDriver(
        TryScheduler(),
        framework,
        MESOS_MASTER_IP,
        use_addict=True,
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print('Scheduler running, Ctrl+C to quit.')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    main()
