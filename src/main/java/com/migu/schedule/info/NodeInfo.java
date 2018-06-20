package com.migu.schedule.info;

import com.migu.schedule.Schedule;

import java.util.ArrayList;

import java.util.List;
/**
 * 节点信息
 */
public class NodeInfo
{
    /**
     * 节点id
     */
    private int nodeId;

    /**
     * 节点资源消耗率
     */
    private int nodeConsumption;

    /**
     * 运行中的任务
     */
    private List<TaskInfo> runTask = new ArrayList<TaskInfo>();


    public int getNodeId()
    {
        return nodeId;
    }

    public void setNodeId(int nodeId)
    {
        this.nodeId = nodeId;
    }

    public void setNodeConsumption(int nodeConsumption)
    {
        this.nodeConsumption = nodeConsumption;
    }

    public int getNodeConsumption()
    {
        return nodeConsumption;
    }

    public boolean addRunTask(TaskInfo task)
    {
        try{
            runTask.add(task);
            nodeConsumption += Schedule.taskConsumption.get(task.getTaskId());
        }
        catch(Exception e){
            return false;
        }
        return true;
    }

    public boolean removeRunTask(int taskId)
    {

        for (int i = 0; i < runTask.size(); i++)
        {
            if(runTask.get(i).getTaskId() == taskId)
            {
                runTask.remove(i);
                nodeConsumption -= Schedule.taskConsumption.get(taskId);
                return true;
            }
        }
        return false;
    }

    public List<TaskInfo> getAllTaks()
    {
        return runTask;
    }
}
