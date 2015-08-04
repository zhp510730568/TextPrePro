package xh.spark.cluster.app

import java.io.FileOutputStream
import xh.spark.cluster.text.TextPreProcess

object App {
	def main(args : Array[String]) {
	  //Utils.exportMatrix("D:/clustercorpus.txt", "D:/matrix.txt")
	  //TextPreProcess.wordDF("D:/clustercorpus.txt", "D:/wordsMap.txt")
	  TextPreProcess.statResult("D:/clustercorpus.txt", "D:/2.txt", "D:/stat1.txt")
	  //TextPreProcess.wordDF("D:/clustercorpus.txt")
	  //TextPreProcess.exportMatrix("D:/clustercorpus.txt", "D:/matrix.txt")
	}
}