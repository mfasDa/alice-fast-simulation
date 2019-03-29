#! /usr/bin/env python

class simtask:

    def __init__(self, executable, defaultargs, optionalargs, jobidstring):
        self.__executable = executable
        self.__defaultargs = defaultargs
        self.__optionalargs = optionalargs
        self.__jobidstring = jobidstring
    
    def create_task_command_serial(self, jobid):
        taskcommand = self.__create_task_command_main()
        taskcommand += " %s %d" %(self.__jobidstring, jobid)
        return "%s" %taskcommand

    def create_task_command_mpi(self):
        taskcommand = self.__create_task_command_main()
        taskcommand += " %s RANK" %(self.__jobidstring)
        return "%s" %taskcommand

    def __create_task_command_main(self):
        taskcommand = self.__executable
        for arg in self.__defaultargs:
            taskcommand += " %s" %arg
        for argk, argv in self.__optionalargs.iteritems():
            taskcommand += " %s" %argk
            if len(argv):
                taskcommand += " %s" %argv
        return taskcommand
