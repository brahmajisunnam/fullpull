package com.disney.wdpr

import scala.io.Source

object Application {

	def main(args: Array[String]): Unit = {

		var configFile: String = ""

		args.sliding(2, 2).toList.collect {
			case Array("--config-file", configParam: String) => configFile = configParam
			case _ => throw new Exception("Unable to parse command line parameters. Parameter passed:[" + args.toList.toString() + "]")
		}

		val listTables = Source.fromFile(configFile).getLines.toList


		for(theTable <- listTables) {
			new HiveLoader(theTable).tryLoading
		}
	}
}
