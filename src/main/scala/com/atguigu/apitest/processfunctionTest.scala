package com.atguigu.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/18 16:28
  */
object processfunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // 读入数据
    //    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )


    val increStream = dataStream
      .keyBy(_.id)
      .process(new TempIncreWarning())
//      .print()

    val monitorStream = dataStream.process(new FreezingMonitor)
    monitorStream.print("main")
    monitorStream.getSideOutput(new OutputTag[String]("freezing-waring")).print("side")

    env.execute("process function test")
  }
}

class TempIncreWarning() extends KeyedProcessFunction[String, SensorReading, String] {
  // 将上一个温度值保存成状态
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))
  // 将注册的定时器时间戳保存成状态
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", Types.of[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出上一次的温度值
    val preTemp = lastTemp.value()
    // 取出定时器的时间戳
    val curTimerTs = currentTimer.value()

    // 更新lastTemp温度值
    lastTemp.update(value.temperature)

    // 假如温度上升，而且没有注册过定时器，注册定时器
    if (value.temperature > preTemp && curTimerTs == 0) {
      val timerTs = ctx.timerService().currentProcessingTime() + 5000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      // 将定时器时间戳保存进状态
      currentTimer.update(timerTs)
    } else if (value.temperature < preTemp) {
      // 如果温度下降，就取消定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }

  // 5秒后定时器触发，输出报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + "传感器温度在5秒内连续上升。")
    currentTimer.clear()
  }
}

class FreezingMonitor extends ProcessFunction[SensorReading, (String, Double)]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, (String, Double)]#Context, out: Collector[(String, Double)]): Unit = {
    if( value.temperature < 32.0 ){
      ctx.output(new OutputTag[String]("freezing-waring"), "Freezing warning")
    }
    // 所有数据都发出到主流
    out.collect( (value.id, value.temperature) )
  }
}