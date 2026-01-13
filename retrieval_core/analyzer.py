"""
Analyzer for evaluation results - provides rich analytics and insights
"""
from typing import List, Dict, Any, Optional
import pandas as pd
import json


class EvaluationAnalyzer:
    """Analyzes evaluation results and provides rich analytics"""

    def __init__(self, results_df: pd.DataFrame):
        """
        Initialize analyzer with evaluation results

        Args:
            results_df: DataFrame with columns: query_text, metrics (JSON string)
        """
        self.results_df = results_df.copy()
        self._parse_metrics()

    def _parse_metrics(self):
        """Parse metrics JSON column into separate columns"""
        if "metrics" not in self.results_df.columns:
            raise ValueError("results_df must have 'metrics' column")

        # Parse metrics JSON
        metrics_list = []
        for idx, row in self.results_df.iterrows():
            metrics_str = row.get("metrics", "{}")
            if isinstance(metrics_str, str):
                try:
                    metrics = json.loads(metrics_str)
                except:
                    metrics = {}
            else:
                metrics = metrics_str or {}
            metrics_list.append(metrics)

        # Add parsed metrics as columns
        metrics_df = pd.DataFrame(metrics_list)
        self.results_df = pd.concat([self.results_df, metrics_df], axis=1)

    def summary(self) -> str:
        """
        Generate summary statistics

        Returns:
            Formatted summary string
        """
        lines = []
        lines.append("=" * 60)
        lines.append("EVALUATION SUMMARY")
        lines.append("=" * 60)

        # Basic stats
        num_queries = len(self.results_df)
        lines.append(f"\nTotal Queries: {num_queries}")

        # Average metrics
        metric_cols = [col for col in self.results_df.columns
                       if any(x in col for x in ['recall', 'precision', 'ndcg', 'relevance', 'judge_score', 'latency'])]

        if metric_cols:
            lines.append("\nAverage Metrics:")
            for col in sorted(metric_cols):
                if col in self.results_df.columns and pd.api.types.is_numeric_dtype(self.results_df[col]):
                    avg_val = self.results_df[col].mean()
                    if 'latency' in col.lower():
                        lines.append(f"  {col}: {avg_val:.2f} ms")
                    elif 'count' in col.lower():
                        lines.append(f"  {col}: {avg_val:.1f}")
                    else:
                        lines.append(f"  {col}: {avg_val:.4f}")

        return "\n".join(lines)

    def score_distribution(self) -> pd.DataFrame:
        """
        Get distribution of relevance scores

        Returns:
            DataFrame with score counts and percentages
        """
        # Find relevance score columns
        score_cols = [col for col in self.results_df.columns
                      if 'relevance_' in col and '_count_' in col]

        if not score_cols:
            # Try to compute from avg_relevance if available
            relevance_cols = [col for col in self.results_df.columns
                              if 'avg_relevance' in col or 'judge_score' in col]

            if relevance_cols:
                # Use first relevance column
                col = relevance_cols[0]
                scores = self.results_df[col].dropna()

                # Bin scores into 0-3 categories
                bins = [-0.5, 0.5, 1.5, 2.5, 3.5]
                labels = [0, 1, 2, 3]
                binned = pd.cut(scores, bins=bins, labels=labels)

                counts = binned.value_counts().sort_index()
                total = len(scores)

                return pd.DataFrame({
                    'score': counts.index,
                    'count': counts.values,
                    'percentage': (counts.values / total * 100).round(2)
                })

        # Use count columns if available
        k_value = None
        for col in score_cols:
            # Extract k value (e.g., "relevance_0_count_at_10" -> k=10)
            parts = col.split('_at_')
            if len(parts) == 2:
                k_value = parts[1]
                break

        if not k_value:
            return pd.DataFrame(columns=['score', 'count', 'percentage'])

        # Sum up counts across all queries
        distribution = {}
        for score in [0, 1, 2, 3]:
            col_name = f'relevance_{score}_count_at_{k_value}'
            if col_name in self.results_df.columns:
                distribution[score] = self.results_df[col_name].sum()

        total = sum(distribution.values())
        if total == 0:
            return pd.DataFrame(columns=['score', 'count', 'percentage'])

        return pd.DataFrame([
            {
                'score': score,
                'count': count,
                'percentage': round(count / total * 100, 2)
            }
            for score, count in sorted(distribution.items())
        ])

    def top_queries(self, n: int = 5, metric: str = None) -> pd.DataFrame:
        """
        Get top N queries by average relevance

        Args:
            n: Number of queries to return
            metric: Metric to sort by (default: auto-detect relevance metric)

        Returns:
            DataFrame with top queries
        """
        if metric is None:
            # Auto-detect relevance metric
            relevance_cols = [col for col in self.results_df.columns
                              if any(x in col for x in ['avg_relevance', 'judge_score', 'recall'])]
            if relevance_cols:
                metric = relevance_cols[0]
            else:
                return pd.DataFrame()

        if metric not in self.results_df.columns:
            return pd.DataFrame()

        # Select relevant columns
        display_cols = ['query_text', metric]
        if 'strategy' in self.results_df.columns:
            display_cols.insert(0, 'strategy')

        df = self.results_df[display_cols].copy()
        df = df.sort_values(by=metric, ascending=False).head(n)
        return df.reset_index(drop=True)

    def bottom_queries(self, n: int = 5, metric: str = None) -> pd.DataFrame:
        """
        Get bottom N queries by average relevance

        Args:
            n: Number of queries to return
            metric: Metric to sort by (default: auto-detect relevance metric)

        Returns:
            DataFrame with bottom queries
        """
        if metric is None:
            # Auto-detect relevance metric
            relevance_cols = [col for col in self.results_df.columns
                              if any(x in col for x in ['avg_relevance', 'judge_score', 'recall'])]
            if relevance_cols:
                metric = relevance_cols[0]
            else:
                return pd.DataFrame()

        if metric not in self.results_df.columns:
            return pd.DataFrame()

        # Select relevant columns
        display_cols = ['query_text', metric]
        if 'strategy' in self.results_df.columns:
            display_cols.insert(0, 'strategy')

        df = self.results_df[display_cols].copy()
        df = df.sort_values(by=metric, ascending=True).head(n)
        return df.reset_index(drop=True)

    def high_relevance_examples(self, n: int = 5, min_score: float = 2.5) -> pd.DataFrame:
        """
        Get examples of high relevance results

        Args:
            n: Number of examples to return
            min_score: Minimum relevance score (default: 2.5)

        Returns:
            DataFrame with high relevance examples
        """
        # Find relevance score column
        relevance_cols = [col for col in self.results_df.columns
                          if 'avg_relevance' in col or 'judge_score' in col]

        if not relevance_cols:
            return pd.DataFrame()

        metric = relevance_cols[0]

        # Filter high relevance
        high_rel = self.results_df[self.results_df[metric] >= min_score].copy()

        if len(high_rel) == 0:
            return pd.DataFrame()

        # Select columns to display
        display_cols = ['query_text', metric]
        if 'strategy' in high_rel.columns:
            display_cols.insert(0, 'strategy')

        return high_rel[display_cols].head(n).reset_index(drop=True)

    def low_relevance_examples(self, n: int = 5, max_score: float = 1.0) -> pd.DataFrame:
        """
        Get examples of low relevance results

        Args:
            n: Number of examples to return
            max_score: Maximum relevance score (default: 1.0)

        Returns:
            DataFrame with low relevance examples
        """
        # Find relevance score column
        relevance_cols = [col for col in self.results_df.columns
                          if 'avg_relevance' in col or 'judge_score' in col]

        if not relevance_cols:
            return pd.DataFrame()

        metric = relevance_cols[0]

        # Filter low relevance
        low_rel = self.results_df[self.results_df[metric] <= max_score].copy()

        if len(low_rel) == 0:
            return pd.DataFrame()

        # Select columns to display
        display_cols = ['query_text', metric]
        if 'strategy' in low_rel.columns:
            display_cols.insert(0, 'strategy')

        return low_rel[display_cols].head(n).reset_index(drop=True)

    def recall_at_k(self, k: int) -> float:
        """
        Get recall@k metric

        Args:
            k: Number of top results

        Returns:
            Recall@k value
        """
        col_name = f'recall_at_{k}'
        if col_name in self.results_df.columns:
            return self.results_df[col_name].mean()
        return 0.0

    def compare_strategies(self, metric: str = 'avg_relevance_at_10') -> pd.DataFrame:
        """
        Compare metrics across strategies

        Args:
            metric: Metric to compare

        Returns:
            DataFrame with strategy comparison
        """
        if 'strategy' not in self.results_df.columns:
            return pd.DataFrame()

        if metric not in self.results_df.columns:
            # Try to find similar metric
            metric_cols = [col for col in self.results_df.columns
                           if 'relevance' in col or 'recall' in col or 'ndcg' in col]
            if metric_cols:
                metric = metric_cols[0]
            else:
                return pd.DataFrame()

        # Group by strategy and compute stats
        grouped = self.results_df.groupby('strategy')[metric].agg(['mean', 'std', 'count']).reset_index()
        grouped.columns = ['strategy', 'avg_' + metric, 'std_' + metric, 'num_queries']
        grouped = grouped.sort_values(by='avg_' + metric, ascending=False)

        return grouped


def compare_evaluations(results_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Compare metrics across multiple evaluation runs (e.g., different strategies or query types)

    Args:
        results_dict: Dictionary mapping name -> results DataFrame

    Returns:
        Comparison DataFrame with metrics for each run
    """
    comparison = []

    for name, df in results_dict.items():
        analyzer = EvaluationAnalyzer(df)

        # Extract key metrics
        metrics = {}
        metrics['name'] = name
        metrics['num_queries'] = len(df)

        # Find available metrics
        metric_cols = [col for col in df.columns
                       if any(x in col for x in ['recall', 'precision', 'ndcg', 'relevance', 'judge_score', 'latency'])]

        for col in metric_cols:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                metrics[col] = df[col].mean()

        comparison.append(metrics)

    if not comparison:
        return pd.DataFrame()

    comp_df = pd.DataFrame(comparison)

    # Sort by avg_relevance or first metric column
    sort_col = None
    for col in comp_df.columns:
        if 'relevance' in col or 'recall' in col:
            sort_col = col
            break

    if sort_col:
        comp_df = comp_df.sort_values(by=sort_col, ascending=False)

    return comp_df
