package org.apache.zeppelin.cassandra

import com.datastax.driver.core.{DataType, Row}
import org.apache.zeppelin.annotation.ZeppelinApi
import org.apache.zeppelin.interpreter.InterpreterContext

/**
  * Template context to hand over the result set of CQL execution
  *
  * TODO A template may need to use bound objects sent by other paragraphs
  */
class TemplateContext(interpreterContext: InterpreterContext,
                      val columnsDefinitions: Seq[(String, DataType)],
                      val rows: Seq[Row]) {

  def notId:String = interpreterContext.getNoteId

  def paragraphId:String = interpreterContext.getParagraphId

  @ZeppelinApi
  def angular(name: String): Any = {
    val ao = getAngularObject(name, interpreterContext)
    if (ao == null) null
    else ao.get
  }

  private def getAngularObject(name: String, interpreterContext: InterpreterContext) = {
    val registry = interpreterContext.getAngularObjectRegistry
    val noteId = interpreterContext.getNoteId
    // try get local object
    val paragraphAo = registry.get(name, noteId, interpreterContext.getParagraphId)
    val noteAo = registry.get(name, noteId, null)
    var ao = if (paragraphAo != null) paragraphAo
    else noteAo
    if (ao == null) {
      // then global object
      ao = registry.get(name, null, null)
    }
    ao
  }
}

