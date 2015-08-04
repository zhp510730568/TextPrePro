package xh.spark.cluster.text

import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import scala.collection.mutable.HashSet
/*
 * 按单词TFIDF值处理语料
 */
object TextPreProcess {  
  /*
   * 计算语料中单词IDF值
   * path: 分词后语料路径
   * maxFreq: 按word\tfreq格式保存到磁盘
   * minFreq: 最低过滤次数
   * maxFreq: 最高过滤次数
   * return: 保存单词IDF值的字典和单词列表的二元组
   */
  def wordDF(path: String, outputpath: String, minFreq: Int, maxFreq: Int) = {
    val bufSource = Source.fromFile(path, "UTF-8")
    val iter = bufSource.getLines
    // 保存单词频次信息
    val wordMap = new HashMap[String, Int]
    // 保存各文章的单词出现频率
    val artiWordList = new MutableList[HashMap[String, Int]]
    iter.foreach(line => {
      val arr = line.split("\t", 3)
      val wordArr = arr(2).split(" ")
      val artiWordMap = new HashMap[String, Int]
      wordArr.foreach(word => {
        artiWordMap.get(word) match {
          case Some(c) => {
            artiWordMap.update(word, c + 1)
          }
          case None => {
            artiWordMap.update(word, 1)
	        wordMap.get(word) match{
	          case Some(x) => wordMap.update(word, x + 1)
	          case None => wordMap.update(word, 1)
	        }
          }
        }
      })
      artiWordList += artiWordMap
    })
    
    //val bufferWritter = new BufferedWriter(new java.io.OutputStreamWriter(new java.io.FileOutputStream(outputpath), "UTF-8"))
    val out = new OutputStreamWriter(new FileOutputStream(new File(outputpath)),"UTF-8");  
    wordMap.foreach(map => {
      val (word, freq) = map
      out.write(word + "\t" + freq + "\r")
    })
    out.flush()
    //bufferWritter.close()
    println(wordMap.size)
    val filterdMap = wordMap.filter(e => {
      val (w, c) = e
      c > minFreq && c < maxFreq
    })
    println(filterdMap.size)
    (filterdMap, artiWordList)
  }
  
  /*
   * 导出语料矩阵，值为单词出现次数
   * datapath: 源语料路径
   * outputpath: 导出数据保存路径
   * minFreq: 过滤最低出现次数
   * maxFreq: 过滤最高出现次数
   * return: Unit
   */
  def exportMatrix_freq(datapath: String, outputpath: String, minFreq: Int, maxFreq: Int) : Unit ={
    // 语料出现的词
    val wordsList = TextPreProcess.wordDF(datapath, "", minFreq, maxFreq)._1.keys.toList
    var str = ""
    wordsList.foreach(word => str += word + " ")
    println(str)
    
    val fileWritter = new FileWriter(new File(outputpath));
    val bufferWritter = new BufferedWriter(fileWritter);
    // 读取语料文章
    val bufSource = Source.fromFile(datapath, "UTF-8")
    val iter = bufSource.getLines
    val artiWordMap = new HashMap[String, Int]()
    val artiVector : StringBuilder = new StringBuilder
    iter.foreach(line => {
      println(line)
      val words = line.split("\t", 3)
      if(words.length == 3) {
        artiWordMap.clear
        artiVector.clear
        words(2).split(" ").foreach(word => {
          artiWordMap.get(word) match {
            case Some(x) => artiWordMap.update(word, x + 1)
            case None => artiWordMap.update(word, 1)
          }
        })
        wordsList.foreach(w => {
          artiWordMap.get(w) match {
            case Some(x) => artiVector.append(x + " ")
            case None => artiVector.append("0 ")
          }
        })
      }
      println(artiVector + "\r")
      bufferWritter.write(artiVector.append("\r").toString)
      bufferWritter.flush()
    })
  }
  
  /*
   * 导出语料矩阵，维值为TFIDF值
   * datapath: 分词后语料路径
   * maxFreq: 按word\tfreq格式保存到磁盘
   * minFreq: 最低过滤次数
   * maxFreq: 最高过滤次数
   * return: 保存单词IDF值的字典和单词列表的二元组
   */
  def exportMatrix_tfidf(datapath: String, outputpath: String, minFreq: Int, maxFreq: Int) : Unit ={
    // 单词TFIDF信息
    val (wordMap, artiWordList) = TextPreProcess.wordDF(datapath, "D:/wordsMap.txt", minFreq, maxFreq)
    val wordList = wordMap.keys.toList
    // 文章总数
    val artiCount = artiWordList.size.toDouble
    
    val bufferWritter = new BufferedWriter(new FileWriter(new File(outputpath)))
    // 读取语料文章
    val bufSource = Source.fromFile(datapath, "UTF-8")
    val iter = bufSource.getLines
    var artiIndex = 0
    iter.foreach(line => {
      println(artiIndex)
      val artiWords = line.split("\t", 3)
      val wordArr = artiWords(2).split(" ")
      val artiWordMap = artiWordList(artiIndex)
      var artiVector = ""
      val totalWordsFreq = artiWordMap.values.reduceLeft(_ + _).toDouble
      wordList.foreach(word => {
        artiWordMap.get(word) match {
          case Some(count) => {
            val tfidf = (count.toDouble / totalWordsFreq) * Math.log(artiCount / (wordMap(word).toDouble))
            artiVector += tfidf + " "
          }
          case None => artiVector += "0.0 "
        }
      })
      artiIndex += 1
      bufferWritter.write(artiVector.trim() + "\r")
      bufferWritter.flush()
    })
    bufferWritter.close()
  }
  
  /*
   * 统计结果，统计结果打印出，“聚类号 分类号 文章标题”格式写到文件中
   * sourcepath: 源语料路径
   * resultpath: 聚类结果文件路径
   * outputPath: 聚类号\t
   * return: Unit
   */
  def statResult(sourcepath: String, resultpath: String, outputPath: String): Unit = {
    val sourcebuf = Source.fromFile(sourcepath, "UTF-8")
    val sourceiter = sourcebuf.getLines
    val resultbuf = Source.fromFile(resultpath, "UTF-8")
    val resultiter = resultbuf.getLines
    // 保存对应数量信息
    val classidMap = new scala.collection.mutable.HashMap[String, HashMap[String, Int]]
    // 保存对应数量信息
    val clusteridMap = new scala.collection.mutable.HashMap[String, HashMap[String, Int]]
    var outputList = new MutableList[(String, String, String)]()
    // 文章数量
    var artiCount = 0
    var classid = ""
    val file = new FileOutputStream(outputPath)
    sourceiter.foreach(line => {
      artiCount += 1
      val articleInfo = line.split("\t", 3)
      classid = articleInfo(0)
      var cluster = resultiter.next
      classidMap.get(classid) match {
        case Some(map) => {
        	map.get(cluster) match {
          	case Some(idcount) => map.update(cluster, idcount + 1)
          	case None => map.update(cluster, 1)
          }
        }
        case None => {
          val newMap = scala.collection.mutable.HashMap[String, Int]()
          newMap.update(cluster, 1)
          classidMap.update(classid, newMap)
        }
      }
      clusteridMap.get(cluster) match {
        case Some(map) => {
          map.get(classid) match {
            case Some(classidcount) => {
              map.update(classid, classidcount + 1)
            }
            case None => {
              map.update(classid, 1)
            }
          } 
        }
        case None => {
          val newMap = scala.collection.mutable.HashMap[String, Int]()
          newMap.update(classid, 1)
          clusteridMap.update(cluster, newMap)
        }
      }
      outputList += ((cluster.toString, classid, articleInfo(1)))
    })
    // 按不同方式排序
    val sortedOutputList = outputList.sortBy(e => e._2)
    
    sortedOutputList.foreach(e => {
      file.write((e._2 + "\t" + e._1 + "\t"+ e._3 + "\r").getBytes())
      file.flush
    })
    file.flush()
    file.close()
    
    println("Article Count:" + artiCount)
    var totalCount = 0
    var classidTotalCount = 0
    var clusterTotalCount = 0
    var correctCount = 0
    var totalCorrectCount = 0
    var outputresult = new MutableList[(String, Int, Int)]()
    
    clusteridMap.foreach(clusterMap => {
      val (key, value) = clusterMap
      totalCount = 0
      correctCount = 0
      value.foreach(classidMap => {
        val (classid, classidCount) = classidMap
        totalCount += classidCount
        if (correctCount < classidCount) {
          correctCount = classidCount
        }
      })
      outputresult += ((key, correctCount, totalCount))
      
      totalCorrectCount += correctCount
    })
    
    outputresult.sortBy(e => e._3).foreach(e => {
      val (clusterid, corrCount, totCount) = e
      println("聚类号:" + clusterid + " \t该簇总数量:" + totCount + " \t正确率:" + corrCount.toDouble / totCount.toDouble)
    })
    
    println("\r\n平均正确率为:" + totalCorrectCount.toDouble / artiCount.toDouble)
  }
}