from typing import List, Dict, Any, Optional
import json
import numpy as np
import requests
import time

class RetrievalEvaluator:
    """Evaluator for retrieval quality metrics"""

    def __init__(self, embedding_endpoint: str = None, judge_model_endpoint: str = None):
        self.embedding_endpoint = embedding_endpoint
        self.judge_model_endpoint = judge_model_endpoint or "databricks-meta-llama-3-1-70b-instruct"
        self._setup_api_client()
    
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

            # Calculate relevant_found once for all metrics to avoid scoping issues
            relevant_found = len(set(top_k_ids) & set(expected_chunk_ids)) if expected_chunk_ids else 0

            # Recall@k: fraction of expected chunks found in top-k
            if expected_chunk_ids:
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
            Dictionary of metrics with scores on 0-3 scale
        """
        metrics = {}

        for k in k_values:
            top_k = retrieved_chunks[:k]
            if not top_k:
                metrics[f"judge_score_at_{k}"] = 0.0
                metrics[f"avg_relevance_at_{k}"] = 0.0
                continue

            # Score each chunk with LLM judge (0-3 scale)
            judge_scores = []
            for chunk in top_k:
                score = self._judge_relevance(query_text, chunk)
                judge_scores.append(score)

            # Average score across top-k results
            avg_score = np.mean(judge_scores) if judge_scores else 0.0
            metrics[f"judge_score_at_{k}"] = avg_score
            metrics[f"avg_relevance_at_{k}"] = avg_score

            # Also compute score distribution
            score_counts = {0: 0, 1: 0, 2: 0, 3: 0}
            for score in judge_scores:
                score_counts[int(score)] = score_counts.get(int(score), 0) + 1

            for score_val, count in score_counts.items():
                metrics[f"relevance_{score_val}_count_at_{k}"] = count

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
    
    def _setup_api_client(self):
        """Setup API client for Databricks Foundation Model API"""
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.core import Config
            import os

            cfg = Config()
            self.w = WorkspaceClient(config=cfg)

            # Try multiple methods to get API token
            self.api_token = (
                getattr(cfg, 'token', None) or
                getattr(cfg, 'auth_token', None) or
                os.environ.get('DATABRICKS_TOKEN') or
                (self.w.config.token if hasattr(self.w, 'config') and hasattr(self.w.config, 'token') else None)
            )

            # Get host URL
            self.api_url = cfg.host or os.environ.get('DATABRICKS_HOST')

            if not self.api_token or not self.api_url:
                print("Warning: Could not retrieve API token or host. LLM judge may not work.")

        except Exception as e:
            print(f"Warning: Could not setup API client: {e}")
            self.w = None
            self.api_token = None
            self.api_url = None

    def _judge_relevance(self, query_text: str, chunk: Dict) -> float:
        """
        Judge relevance using LLM on a 0-3 scale

        Scale:
        - 0: Not relevant - The chunk has no useful information for the query
        - 1: Marginally relevant - The chunk mentions related concepts but doesn't answer the query
        - 2: Relevant - The chunk contains useful information that partially answers the query
        - 3: Highly relevant - The chunk directly and completely answers the query

        Args:
            query_text: User query
            chunk: Retrieved chunk dictionary

        Returns:
            Relevance score (0.0 to 3.0)
        """
        # Extract chunk text
        chunk_text = chunk.get("chunk_text", chunk.get("text", ""))
        if not chunk_text:
            return 0.0

        # Truncate if too long (to stay within token limits)
        max_chunk_length = 2000
        if len(chunk_text) > max_chunk_length:
            chunk_text = chunk_text[:max_chunk_length] + "..."

        # Build prompt for LLM judge
        prompt = f"""You are evaluating the relevance of a search result for a given query.

Query: {query_text}

Retrieved Text:
{chunk_text}

Rate the relevance of this text to the query on a scale of 0-3:
- 0: Not relevant - No useful information for the query
- 1: Marginally relevant - Mentions related concepts but doesn't answer the query
- 2: Relevant - Contains useful information that partially answers the query
- 3: Highly relevant - Directly and completely answers the query

Respond with ONLY a single digit (0, 1, 2, or 3), nothing else."""

        try:
            # Call Databricks Foundation Model API
            score = self._call_llm_api(prompt)
            return float(score)
        except Exception as e:
            print(f"Warning: LLM judge failed with error: {e}")
            # Fallback to keyword overlap heuristic
            return self._fallback_relevance_score(query_text, chunk_text)

    def _call_llm_api(self, prompt: str, max_retries: int = 3) -> int:
        """
        Call Databricks Foundation Model API

        Args:
            prompt: Prompt for the model
            max_retries: Maximum number of retries

        Returns:
            Relevance score (0-3)
        """
        if not self.api_token or not self.api_url:
            raise ValueError("API client not configured")

        endpoint_url = f"{self.api_url}/serving-endpoints/{self.judge_model_endpoint}/invocations"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 10,
            "temperature": 0.0
        }

        for attempt in range(max_retries):
            try:
                response = requests.post(endpoint_url, json=payload, headers=headers, timeout=30)
                response.raise_for_status()

                result = response.json()

                # Extract score from response
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0].get("message", {}).get("content", "")
                elif "predictions" in result and len(result["predictions"]) > 0:
                    content = result["predictions"][0].get("content", "")
                else:
                    content = str(result)

                # Parse score (handle various formats)
                content = content.strip()

                # Try to extract first digit
                import re
                match = re.search(r'[0-3]', content)
                if match:
                    score = int(match.group())
                    if 0 <= score <= 3:
                        return score

                # If no valid score found, return 0
                print(f"Warning: Could not parse score from response: {content}")
                return 0

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                raise
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise

        return 0

    def _fallback_relevance_score(self, query_text: str, chunk_text: str) -> float:
        """
        Fallback relevance scoring using keyword overlap

        Args:
            query_text: User query
            chunk_text: Retrieved chunk text

        Returns:
            Estimated relevance score (0.0 to 3.0)
        """
        # Simple keyword overlap heuristic
        query_words = set(query_text.lower().split())
        chunk_words = set(chunk_text.lower().split())

        if not query_words:
            return 0.0

        overlap = len(query_words & chunk_words) / len(query_words)

        # Map overlap to 0-3 scale
        if overlap >= 0.7:
            return 3.0
        elif overlap >= 0.4:
            return 2.0
        elif overlap >= 0.2:
            return 1.0
        else:
            return 0.0
    
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

