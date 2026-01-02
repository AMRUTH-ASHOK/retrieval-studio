from typing import List, Dict, Any
import json
import numpy as np

class RetrievalEvaluator:
    """Evaluator for retrieval quality metrics"""
    
    def __init__(self, embedding_endpoint: str = None, judge_model_endpoint: str = None):
        self.embedding_endpoint = embedding_endpoint
        self.judge_model_endpoint = judge_model_endpoint
    
    def compute_labeled_metrics(
        self,
        query_text: str,
        retrieved_chunks: List[Dict],
        expected_chunk_ids: List[str],
        k_values: List[int] = [5, 10]
    ) -> Dict[str, float]:
        """
        Compute metrics when ground truth is available
        
        Args:
            query_text: Query text
            retrieved_chunks: List of retrieved chunks with 'chunk_id' and 'score'
            expected_chunk_ids: List of expected chunk IDs
            k_values: List of k values for metrics
        
        Returns:
            Dictionary of metrics
        """
        metrics = {}
        
        # Extract chunk IDs from retrieved chunks
        retrieved_ids = [chunk.get("chunk_id", chunk.get("id", "")) for chunk in retrieved_chunks]
        
        for k in k_values:
            top_k_ids = retrieved_ids[:k]
            
            # Recall@k: fraction of expected chunks found in top-k
            if expected_chunk_ids:
                relevant_found = len(set(top_k_ids) & set(expected_chunk_ids))
                recall = relevant_found / len(expected_chunk_ids) if expected_chunk_ids else 0.0
                metrics[f"recall_at_{k}"] = recall
            else:
                metrics[f"recall_at_{k}"] = 0.0
            
            # Precision@k: fraction of top-k that are relevant
            if expected_chunk_ids:
                precision = relevant_found / k if k > 0 else 0.0
                metrics[f"precision_at_{k}"] = precision
            else:
                metrics[f"precision_at_{k}"] = 0.0
            
            # NDCG@k: Normalized Discounted Cumulative Gain
            ndcg = self._compute_ndcg(top_k_ids, expected_chunk_ids, k)
            metrics[f"ndcg_at_{k}"] = ndcg
        
        return metrics
    
    def compute_judge_metrics(
        self,
        query_text: str,
        retrieved_chunks: List[Dict],
        k_values: List[int] = [5, 10]
    ) -> Dict[str, float]:
        """
        Compute metrics using LLM judge when ground truth is not available
        
        Args:
            query_text: Query text
            retrieved_chunks: List of retrieved chunks
            k_values: List of k values for metrics
        
        Returns:
            Dictionary of metrics
        """
        metrics = {}
        
        # If judge model endpoint is available, use it
        if self.judge_model_endpoint:
            # Placeholder for LLM judge implementation
            # Would call the judge endpoint to score relevance
            for k in k_values:
                top_k = retrieved_chunks[:k]
                # Simulated judge scores (0-1)
                judge_scores = [self._judge_relevance(query_text, chunk) for chunk in top_k]
                metrics[f"judge_score_at_{k}"] = np.mean(judge_scores) if judge_scores else 0.0
        else:
            # Fallback: use simple heuristics
            for k in k_values:
                metrics[f"judge_score_at_{k}"] = 0.5  # Placeholder
        
        return metrics
    
    def _compute_ndcg(self, retrieved_ids: List[str], relevant_ids: List[str], k: int) -> float:
        """Compute NDCG@k"""
        if not relevant_ids:
            return 0.0
        
        # Compute DCG
        dcg = 0.0
        for i, chunk_id in enumerate(retrieved_ids[:k]):
            if chunk_id in relevant_ids:
                # Discounted gain: 1 / log2(i+2) for position i
                dcg += 1.0 / np.log2(i + 2)
        
        # Compute IDCG (ideal DCG)
        idcg = 0.0
        for i in range(min(len(relevant_ids), k)):
            idcg += 1.0 / np.log2(i + 2)
        
        # NDCG = DCG / IDCG
        return dcg / idcg if idcg > 0 else 0.0
    
    def _judge_relevance(self, query_text: str, chunk: Dict) -> float:
        """
        Judge relevance using LLM (placeholder)
        
        In real implementation, this would call the judge model endpoint
        """
        # Placeholder - would call judge endpoint
        return 0.5
    
    def aggregate_metrics(self, results: List[Dict]) -> Dict[str, float]:
        """
        Aggregate metrics across multiple queries
        
        Args:
            results: List of result dictionaries, each with 'metrics' key containing JSON string
        
        Returns:
            Dictionary of aggregated metrics
        """
        all_metrics = []
        
        for result in results:
            metrics_str = result.get("metrics", "{}")
            if isinstance(metrics_str, str):
                metrics = json.loads(metrics_str)
            else:
                metrics = metrics_str
            all_metrics.append(metrics)
        
        if not all_metrics:
            return {}
        
        # Aggregate by averaging
        aggregated = {}
        metric_keys = set()
        for m in all_metrics:
            metric_keys.update(m.keys())
        
        for key in metric_keys:
            values = [m.get(key, 0.0) for m in all_metrics if key in m]
            aggregated[f"avg_{key}"] = np.mean(values) if values else 0.0
        
        return aggregated

