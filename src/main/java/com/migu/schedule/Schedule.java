package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.NodeInfo;
import com.migu.schedule.info.TaskInfo;
import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
*类名和方法不能修改
 */
public class Schedule {

    /**
     * 所有的注册节点
     */
    private static List<NodeInfo> allNodeInfo = new ArrayList<NodeInfo>();

    /**
     *  挂起任务
     */
    private static List<TaskInfo> toBeRunTaskInfo = new ArrayList<TaskInfo>();

    /**
     * 任务阈值
     */
    public static Map<Integer, Integer> taskConsumption = new ConcurrentHashMap<Integer, Integer>();


    /**
     * 注册节点任务清空，注册节点清空，挂起任务清空，
     * @return
     */
    public int init() {

        allNodeInfo = new ArrayList<NodeInfo>();
        toBeRunTaskInfo = new ArrayList<TaskInfo>();

        return ReturnCodeKeys.E001;
    }

    /**
     * 节点注册，将注册机器添加到注册节点列表，此时该节点内运行任务为空
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId) {

        //节点编号非法
        if(nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }

        //节点已经注册
        for(NodeInfo nodeInfo :allNodeInfo)
        {
            if(nodeId == nodeInfo.getNodeId())
            {
                return ReturnCodeKeys.E005;
            }
        }

        //正常注册
        NodeInfo node = new NodeInfo();
        node.setNodeId(nodeId);
        allNodeInfo.add(node);

        return ReturnCodeKeys.E003;
    }

    /**
     * 节点注销
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId) {
        //节点编号非法
        if(nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }

        //节点未注册
        boolean register = false;
        NodeInfo registerNodeInfo = null;
        for(NodeInfo nodeInfo :allNodeInfo)
        {
            if(nodeId == nodeInfo.getNodeId())
            {
                register = true;
                registerNodeInfo = nodeInfo;
                break;
            }
        }
        if(!register)
        {
            return ReturnCodeKeys.E007;
        }

        /**节点注销
         *1、将正在运行任务改为挂起
         * 2、删除节点
         */
        toBeRunTaskInfo.addAll(registerNodeInfo.getAllTaks());
        allNodeInfo.remove(registerNodeInfo);

        return ReturnCodeKeys.E006;
    }


    /**
     * 任务添加到挂起队列
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption) {
        //任务编号非法
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        }

        //任务已经添加
        boolean register = false;
        for (TaskInfo taskInfo : toBeRunTaskInfo) {
            if (taskId == taskInfo.getTaskId()) {
                return ReturnCodeKeys.E010;
            }
        }

        //任务添加到挂起队列
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId(taskId);
        toBeRunTaskInfo.add(taskInfo);

        taskConsumption.put(taskId, consumption);

        return ReturnCodeKeys.E008;
    }


    /**
     * 删除挂起任务及运行任务
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId) {


        if(taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }

        //挂起任务删除
        for(TaskInfo taskInfo : toBeRunTaskInfo)
        {
            if(taskInfo.getTaskId() == taskId)
            {
                toBeRunTaskInfo.remove(taskInfo);
                taskConsumption.remove(taskId);
                return ReturnCodeKeys.E011;
            }
        }

        //删除所有服务器运行任务
        for(NodeInfo nodeInfo: allNodeInfo)
        {
            for(TaskInfo taskInfo : nodeInfo.getAllTaks())
            {
                if(taskInfo.getTaskId() == taskId)
                {
                    toBeRunTaskInfo.remove(taskInfo);
                    taskConsumption.remove(taskId);
                    return ReturnCodeKeys.E011;
                }
            }
        }

        // 挂起任务及服务器任务都不存在
        return ReturnCodeKeys.E012;
    }


    /**
     * 调度任务核心
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold) {

        if(threshold <=0 )
        {
            return ReturnCodeKeys.E002;
        }

        //备份挂起任务
        List<TaskInfo> backToBeRunTaskInfo = new ArrayList<TaskInfo>();
        Collections.copy(toBeRunTaskInfo, backToBeRunTaskInfo);

        for(TaskInfo taskInfo : backToBeRunTaskInfo)
        {
            NodeInfo lessConsumptionNode = findLessConsumptionNode();
            lessConsumptionNode.addRunTask(taskInfo);
            //超阈值直接返回无合适方案
            if(isOverTop(threshold))
            {
                return ReturnCodeKeys.E014;
            }
            toBeRunTaskInfo.remove(taskInfo);
        }





        return ReturnCodeKeys.E013;
    }


    /**
     * 查找各点相减后是否超阈值
     * @param threshold
     * @return
     */
    private boolean isOverTop(int threshold)
    {
        for(NodeInfo nodeInfo : allNodeInfo)
        {
            for(NodeInfo secNodeInfo : allNodeInfo)
            {
                //不比较自己
                if(nodeInfo == secNodeInfo)
                {
                    continue;
                }

                if(nodeInfo.getNodeConsumption() - secNodeInfo.getNodeConsumption() > threshold)
                {
                    return true;
                }

            }
        }


        return false;
    }

    /**
     * 查找最小消耗率节点机器
     * 相同大小情况下，取最大的node id
     * @return
     */
    private NodeInfo findLessConsumptionNode()
    {
        int tempConsumption = Integer.MAX_VALUE;
        NodeInfo  tempNodeInfo = null;
        //升序
        Collections.sort(allNodeInfo, new Comparator<NodeInfo>() {
            public int compare(NodeInfo o1, NodeInfo o2) {
                return o1.getNodeId() - o2.getNodeId();
            }
        });

        for(NodeInfo nodeInfo : allNodeInfo)
        {
            if(nodeInfo.getNodeConsumption() <= tempConsumption)
            {
                tempConsumption = nodeInfo.getNodeConsumption();
                tempNodeInfo = nodeInfo;
            }
        }
        return tempNodeInfo;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {

        if(null == tasks)
        {
            return ReturnCodeKeys.E016;
        }


            //挂起任务查询
            for(TaskInfo toBeRuntaskInfo : toBeRunTaskInfo)
            {
                    //如果该任务处于挂起队列中, 所属的服务编号为-1;
                    toBeRuntaskInfo.setNodeId(-1);
                    tasks.add(toBeRuntaskInfo);
            }

            //删服务器运行任务查询
            for(NodeInfo nodeInfo: allNodeInfo)
            {
                for(TaskInfo nodeAllTaskInfo : nodeInfo.getAllTaks())
                {
                    tasks.add(nodeAllTaskInfo);
                }
            }


        //task id升序排列
        Collections.sort(tasks, new Comparator<TaskInfo>() {
            public int compare(TaskInfo o1, TaskInfo o2) {
                return  o2.getTaskId() - o1.getTaskId();
            }
        });

        return ReturnCodeKeys.E015;
    }

}
