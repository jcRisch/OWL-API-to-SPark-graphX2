import java.io.{File, InputStream, OutputStream}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._

import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.io.{OWLOntologyDocumentSource, OWLOntologyDocumentTarget, OWLParserFactory}
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.model.parameters.{OntologyCopy, ChangeApplied}
import org.semanticweb.owlapi.util.{SimpleIRIMapper, PriorityCollection}

/**
 * Created by jerisch on 06/08/2015.
 */
object Main {
  def main(args: Array[String]): Unit = {

    // INITIALISATION SPARK

    val conf = new SparkConf().setAppName("HHHH").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // CHARGEMENT DE L'ONTOLOGIE

    val manager: OWLOntologyManager = OWLManager.createOWLOntologyManager()
    val source: File = new File("C:/zzzzzzz/ontology.owl")
    val ontology: OWLOntology = manager.loadOntologyFromOntologyDocument(source)

    // ON BOUCLE SUR L ONTOLOGIE
    var allVertex: Map[String, Long] = Map()
    var allVertexReverse: Map[Long, String] = Map()
    val vertexArray = Array[Tuple2[Long, String]]()
    var i: Long = 0

    // On recupere les sommets
    for(individual <- ontology.getClassesInSignature()) {
      allVertex += (individual.getIRI.getShortForm -> i)
      allVertexReverse += (i -> individual.getIRI.getShortForm)
      vertexArray :+ (i, individual.getIRI.getShortForm)
      i += 1
    }

    // On ne peut pas utiliser zipWithIndex car retourne un Int et besoins d'un Long
    /*
    ontology.getClassesInSignature().zipWithIndex foreach{ case(ind, compteur) =>

    }
    */

    val edges = Array.ofDim[Long](ontology.getAxioms(AxiomType.SUBCLASS_OF).size(),2)
    val edgesArray = Array[Edge[String]]()
    var j = 0

    // On recupere les arcs
    for(truc <- ontology.getAxioms(AxiomType.SUBCLASS_OF)) {
      // println("super : " + truc.getAxiomWithoutAnnotations.getSuperClass.asOWLClass().getIRI.getShortForm + " et sous : " + truc.getAxiomWithoutAnnotations.getSubClass.asOWLClass().getIRI.getShortForm)
      edges(j)(0) = allVertex(truc.getAxiomWithoutAnnotations.getSuperClass.asOWLClass().getIRI.getShortForm)
      edges(j)(1) = allVertex(truc.getAxiomWithoutAnnotations.getSubClass.asOWLClass().getIRI.getShortForm)
      edgesArray :+ Edge(allVertex(truc.getAxiomWithoutAnnotations.getSuperClass.asOWLClass().getIRI.getShortForm), allVertex(truc.getAxiomWithoutAnnotations.getSubClass.asOWLClass().getIRI.getShortForm), "subClassOf")
      j += 1
    }

    // On cree les RDDs
    val vertexRDD: RDD[(VertexId, (String))] = sc.parallelize(vertexArray)
    val edgesRDD: RDD[Edge[String]] = sc.parallelize(edgesArray)
    val graph = Graph(vertexRDD, edgesRDD)

    // On affiche les arcs
    edges.foreach(edge => {
      println("Nouveau :")
      println("Super : " + edge(0) + " - > " + allVertexReverse(edge(0)) + " | Sous : " + edge(1) + " - > " + allVertexReverse(edge(1)))
      println("")
    })
  }
}
