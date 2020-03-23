package com.boyun.lineage


import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogRelation

import scala.collection.mutable._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.CreateTable

import scala.collection.mutable

class ParseLogistic {
  def resolveLogicPlan(plan: LogicalPlan, currentDB: String): (Set[DcTable], Set[DcTable]) = {
    val inputTables = new mutable.HashSet[DcTable]()
    val outputTables = new HashSet[DcTable]()
    resolveLogic(plan, currentDB, inputTables, outputTables)
    Tuple2(inputTables, outputTables)
  }

  def resolveLogic(plan: LogicalPlan, currentDB: String, inputTables: Set[DcTable], outputTables: Set[DcTable]): Unit = {
    plan match {

      case plan: Project =>
        val project = plan.asInstanceOf[Project]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Union =>
        val project = plan.asInstanceOf[Union]
        for (child <- project.children) {
          resolveLogic(child, currentDB, inputTables, outputTables)
        }

      case plan: Join =>
        val project = plan.asInstanceOf[Join]
        resolveLogic(project.left, currentDB, inputTables, outputTables)
        resolveLogic(project.right, currentDB, inputTables, outputTables)

      case plan: Aggregate =>
        val project = plan.asInstanceOf[Aggregate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Filter =>
        val project = plan.asInstanceOf[Filter]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Generate =>
        val project = plan.asInstanceOf[Generate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: RepartitionByExpression =>
        val project = plan.asInstanceOf[RepartitionByExpression]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SerializeFromObject =>
        val project = plan.asInstanceOf[SerializeFromObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapPartitions =>
        val project = plan.asInstanceOf[MapPartitions]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: DeserializeToObject =>
        val project = plan.asInstanceOf[DeserializeToObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Repartition =>
        val project = plan.asInstanceOf[Repartition]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      /*case plan: Deduplicate =>
        val project = plan.asInstanceOf[Ded uplicate]
        resolveLogic(project.child, currentDB, inputTables, outputTables)*/

      case plan: Window =>
        val project = plan.asInstanceOf[Window]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapElements =>
        val project = plan.asInstanceOf[MapElements]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: TypedFilter =>
        val project = plan.asInstanceOf[TypedFilter]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Distinct =>
        val project = plan.asInstanceOf[Distinct]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SubqueryAlias =>
        val project = plan.asInstanceOf[SubqueryAlias]
        val childInputTables = new HashSet[DcTable]()
        val childOutputTables = new HashSet[DcTable]()

        resolveLogic(project.child, currentDB, childInputTables, childOutputTables)
        if (childInputTables.size > 0) {
          for (table <- childInputTables) inputTables.add(table)
        } else {
          inputTables.add(DcTable(currentDB, project.alias))
        }

      case plan: CatalogRelation =>
        val project = plan.asInstanceOf[CatalogRelation]
        //val identifier = project.tableMeta.identifier
        val identifier = project.catalogTable.identifier
        val dcTable = DcTable(identifier.database.getOrElse(currentDB), identifier.table)
        inputTables.add(dcTable)

      case plan: UnresolvedRelation =>
        val project = plan.asInstanceOf[UnresolvedRelation]
        val dcTable = DcTable(project.tableIdentifier.database.getOrElse(currentDB), project.tableIdentifier.table)
        inputTables.add(dcTable)

      case plan: InsertIntoTable =>
        val project = plan.asInstanceOf[InsertIntoTable]
        resolveLogic(project.table, currentDB, outputTables, inputTables)
        //resolveLogic(project.query, currentDB, inputTables, outputTables)
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: CreateTable =>
        val project = plan.asInstanceOf[CreateTable]
        if (project.query.isDefined) {
          resolveLogic(project.query.get, currentDB, inputTables, outputTables)
        }
        val tableIdentifier = project.tableDesc.identifier
        val dcTable = DcTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
        outputTables.add(dcTable)

      case plan: GlobalLimit =>
        val project = plan.asInstanceOf[GlobalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: LocalLimit =>
        val project = plan.asInstanceOf[LocalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case `plan` => println("******child plan******:\n" + plan)
    }
  }

}

case class DcTable(dc: String, table: String) {

}
