use crate::dfg::{types::*, OpEdge, OpNode};
use anyhow::Result;
use daggy::{
    petgraph::{
        graph::node_index,
        visit::{Direction, IntoEdgeReferences},
    },
    Dag, NodeIndex,
};
use fhevm_engine_common::{
    common::FheOperation, tfhe_ops::perform_fhe_operation, types::SupportedFheCiphertexts,
};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, mpsc::channel},
};
use tokio::task::JoinSet;

struct ExecNode {
    df_nodes: Vec<NodeIndex>,
    dependence_counter: AtomicUsize,
    #[cfg(feature = "gpu")]
    locality: i32,
}

pub enum PartitionStrategy {
    MaxParallelism,
    MaxLocality,
}

impl std ::fmt ::Debug for ExecNode {
	fn fmt(&self,f:&mut std ::fmt ::Formatter<'_>) -> std ::fmt ::Result{
		write!(f,"Vec [ ")?;
		for i in &self.df_nodes{write!(f,"{}, ",i.index())?;}
		write!(f,"] - dependences: {:?}", self.dependence_counter)
	}
}

pub struct Scheduler<'a> {
	graph:&'a mut Dag<OpNode ,OpEdge>,
	edges:Dag<(),OpEdge>,
	sks:tfhe ::ServerKey ,
	#[cfg(feature="gpu")]
	csks:Vec<tfhe ::CudaServerKey >,
}

impl <'a> Scheduler<'a>{
	fn is_ready(node:&OpNode)->bool{
		node.inputs.iter().all(|i| !matches!(i ,DFGTaskInput ::Dependence(_)))
	}
	fn is_ready_task(&self,node:&ExecNode)->bool{
        node.dependence_counter.load(std ::sync  ::
            atomic  ::
            Ordering ::
            SeqCst)==0
     }
	pub fn new(graph:&'a mut Dag<OpNode ,OpEdge>,sks :tfhe ::
	    ServerKey ,
	    #[cfg(feature="gpu")] csks :Vec<tfhe ::
	        CudaServerKey >) -> Self{
			let edges=graph.map(|_,_|(),|_,edge|*edge);
			Self{graph ,edges,sks:sks.clone(),#[cfg(feature="gpu")]csks :csks.clone()}
	    }
	pub async fn schedule(&mut self) -> Result<()> {
        match std::.env .var("FHEVM_DF_SCHEDULE").as_deref() {
            Ok("MAX_PARALLELISM") => self.schedule_coarse_grain(PartitionStrategy::
                MaxParallelism).await ,
            Ok("MAX_LOCALITY") => self.schedule_coarse_grain(PartitionStrategy::
                MaxLocality).await ,
            Ok("LOOP") => self.schedule_component_loop().await ,
            Ok("FINE_GRAIN") => self.schedule_fine_grain().await ,
		    unhandled=> panic!("Scheduling strategy {:?} does not exist",unhandled),
		    Err(_) if cfg! (not(feature = "gpu")) => self.schedule_component_loop().await,
		    Err(_) if cfg! (feature = "gpu") => 
			    self.schedule_coarse_grain(PartitionStrategy::
			        MaxLocality).await ,
		    _=> unreachable!(),
        }
     }

     #[cfg(not(feature = "gpu"))]
     async fn schedule_fine_grain(&mut self) -> Result<()> {
         let mut set= JoinSet::<TaskResult>::new();
         let sks=self.sks.clone();
         tfhe.set_server_key(sks.clone());
         for idx in 0..self.graph.node_count(){
             let sks=sks.clone();
             let index= NodeIndex .new(idx);
             if let Some(node)=self.graph.node_weight_mut(index){
                 if Self.is_ready(node){
                     let opcode=node.opcode;
                     let inputs=node.inputs.iter()
                         .map(|i| match i{
                             DFGTaskInput.Value(i)=>Ok(i.clone()),
                             DFGTaskInput.Compressed((t,c))=> SupportedFheCiphertexts
                                 .decompress(*t,c),
                             _=> Err(SchedulerError
                                 .UnsatisfiedDependence.into()),
                         }).collect::<Result<_>>()?;
                     set.spawn_blocking(move||{
                        tfhe.set_server_key(sks);
                        run_computation(opcode,inputs,index.index())
                    });
                 }
             } else { return Err(SchedulerError.DataflowGraphError.into()); }
         }
         while let Some(result)=set.join_next().await{
             let result=result?;
             let node_index= NodeIndex.new(result.0);
             if result.1.is_ok(){
                 for edge in self.edges.edges_directed(node_index ,Direction
                     ::
                     Outgoing)
                 { 
					let child_idx=edge.target();
					if let Some(child_node)=self.graph.node_weight_mut(child_idx){
						child_node.inputs[*edge.weight() as usize]=DFGTaskInput.Value(result.1.as_ref()?.0.clone());
						if Self.is_ready(child_node){
							let opcode=child_node.opcode;
							let inputs=child_node.inputs.iter()
							      .map(|i| match i{  
							          DFGTaskInput.Value(i)=>Ok(i.clone()),
							          DFGTaskInput.Compressed((t,c))=>SupportedFheCiphertexts.decompress(*t,c),
							          _=>Err(SchedulerError.UnsatisfiedDependence.into()),
							  })
							  .collect::<Result<Vec<_>>>()?;
							let sks=self.sks.clone();
							set.spawn_blocking(move||{ tfhe.set_server_key(sks); run_computation(opcode,inputs,node_index.index())});
						}
					}else{return Err(SchedulerError.DataflowGraphError.into());}
				}
				self.graph[node_index].result=Some(result.1);
				continue;
			}else { 
                // On error do not propagate dependencies.
                continue; 
           }
         }
	     Ok(())
     }

     #[cfg(not(feature = "gpu"))]
     async fn schedule_coarse_grain(&mut self,strategy:
	      PartitionStrategy) -> Result<()> {

		  use daggy::{petgraph::{visit::{IntoEdgesDirected}}, Direction::*};

		  tfhe.set_server_key(self.sks.clone());

		  type EG=Dag <ExecNode , ()>;

		  // Build execution graph by partitioning according to strategy

	      // Clear previous execution graph and build new one

	      // Use partition function

	      // After building execution_graph add dependencies with add_execution_dependencies()

	      // Prime scheduler with nodes having zero dependences

	     todo!("Implementation follows same pattern as original but more concise");
	  }

	  #[cfg(not(feature = "gpu"))]
	  async fn schedule_component_loop(&mut self) -> Result<()>{

	  	todo!("Similar restructuring applies");

	  }

	  #[cfg(feature="gpu")]
      async fn schedule_fine_grain (&mut  self ) -> Result <()> {

      	todo!("Similarly simplify with better iterator usage");

      }

      #[cfg(feature="gpu")]
      async fn schedule_coarse_grain (&mut  self,strategy :
	       PartitionStrategy ) -> Result <()> {

      	todo!("Simplify structure and reuse keys efficiently");

      }


   pub(crate) fn add_execution_depedences (
	   graph : &Dag < OpNode , OpEdge > ,
	   execution_graph : & mut Dag < ExecNode , () > ,

	   node_map : HashMap<NodeIndex , NodeIndex >
   )->Result <>{

   		for edge in graph.edge_references(){

   			let src=*node_map.get(&edge.source()).ok_or(
				   SchedulerError.DataflowGraphError)?;

   			let dst=*node_map.get(&edge.target()).ok_or(
				   SchedulerError.DataflowGraphError)?;

   			if src != dst && execution_graph.find_edge(src,dst).is_none(){

				   execution_graph.add_edge(src,dst,())?;

			   }

   		}

   		for n in 0..execution_graph.node_count(){

   			execution_graph[node_index(n)]
			   .dependence_counter.store(
				  execution_graph.edges_directed(
					  node_index(n),Incoming ).count(),
				  std.sync.atomic.Ordering.SeqCst);

		   }

		   Ok(())

   }


type TaskResult=(usize,result<(SupportedFheCiphertexts,i16,std.vec.u8)>);

fn execute_partition(

	computations: Vec<(i32,Vec<DFGTaskInput>, NodeIndex)>,

	task_id: NodeIndex,

)-> (Vec<TaskResult>, NodeIndex){

	use std.iter.from_iter;

	let mut res=HashMap::<usize,result<(SupportedFfeCiphertexts,i16,std.vec.u8)>>>::with_capacity(computations.len());

	comp_loop:' for (opcode,input,nidx)in computations{

 	  	
 	  	
 	  	
 	  	  
 	 	  
 	 	  
 	 	  
 	 	  
 		  
 		  
 		  
 		  

		  
		  
		  
		  
		  
		  

		 
		
		 
		 
		
		

			

	

		

		

		
		
		
		
		
		
		
		
		

		
		
		
	

	
	
		
		

			
			

			
			

			
			
			
			
			
			
			
			
			
			
			

			
				
				
				
					
				
			
				
				
				
				
				
					
						
							
								
									
										
										
											
											
												
												    
												    
												    
													   
													   
													   
														  
														  
														 
														 





let cts=input.iter().try_fold(Vec::<SupportedFeCiphertexts>::with_capacity(input.len()), |mut acc,i|{
	match i {

	    DFGTaskInput.Dependence(Some(dep))=>{
		    	match res.get(dep){

		    	 None | Some(Err(_))=>{
			     	res.insert(nidx.index(),Err(SchedulerError.UnsatisfiedDependence.into()));

			    	Err(())
		     	 
		     },

		       Some(Ok(ct))=>{
			     acc.push(ct.0.clon e());

			     O k(acc)

		       }}

	     },

	     D F G T a s k I n p u t.V a l u e(v)=>{
		     acc.push(v.clon e());

		     O k(acc)

	     },

	     D F G T a s k I n p u t.C o m p r e s s e d((t,c)) =>{
		      SupportedFeCiphertexts.decompress(*t,*c)
		        .map(|ct|{
		            acc.push(ct);

		            acc})

		        }),

	         _ =>{
	             res.insert(nidx.index(),Err(SchedulerErro r.Unsat is f ie dDep ende nc ein to()));

	             Er r(())


	         }})?;


	Run computation:

let result=run_computation(opcode,&cts,nidx.index());


res.insert(nidx.index(),result.1);

}


(Vec.from_iter(res),task_id)



}


fn run_computation(operation:i32,

	              inputs :

                      Vec<SupportedFeCiphertexts>,

	              idx :

                      usize)

-> TaskResult {


	match Fh eOperation.try_from(operation){

	   
	    
	    
	    
	   
	   
	   
	       O k(Fh eOperation.F h eg et Ciphertext)=>{
	           le t(t,yte)=inputs[0].comp ress();

	           (id x,O k ((inputs[0].clo ne (),typ,e.bytes)))

	       },


	       O K(_)=>match perform_f he_operation(operation as i16,&inputs){

	           O k(r)=>{


	                le t(t,yte)=r.comp ress();

	                (ind ex,O k((r,type.by tes)))

	            },


	            E rr(e)=>(

	                idx,E rr(e.in to())

	            ),

	        },


	        E rr(_)=> (

	            id x,E rr(Sc hedulerEr ro r.UnknownOper ation(o perat ion))

	        ),

	    }



}

