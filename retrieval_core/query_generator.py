"""
Query Generator - Automatically generates evaluation queries from document corpus
"""
from typing import List, Dict, Any, Optional
import random
import requests
import time
import json


class QueryGenerator:
    """Generates synthetic queries from document corpus using LLM"""

    def __init__(self, model_endpoint: str = "databricks-claude-sonnet-4-5", random_seed: int = None):
        """
        Initialize query generator

        Args:
            model_endpoint: Databricks Foundation Model endpoint name
            random_seed: Random seed for reproducibility
        """
        self.model_endpoint = model_endpoint
        self.random_seed = random_seed
        self.few_shot_examples = []

        if random_seed is not None:
            random.seed(random_seed)

        self._setup_api_client()

    def _setup_api_client(self):
        """Setup OpenAI client for Databricks serving endpoints"""
        try:
            from openai import OpenAI

            # Get token from notebook context
            try:
                import IPython
                dbutils = IPython.get_ipython().user_ns.get('dbutils')
                if dbutils:
                    self.api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
                else:
                    self.api_token = None
            except:
                self.api_token = None

            if not self.api_token:
                print("❌ Could not get API token from notebook context")
                self.client = None
                return

            # Initialize OpenAI client with Databricks endpoint
            self.client = OpenAI(
                api_key=self.api_token,
                base_url="https://cust-e2-us-west-2.cloud.databricks.com/serving-endpoints"
            )
            print(f"✅ QueryGenerator initialized with OpenAI client")

        except Exception as e:
            print(f"❌ Failed to initialize OpenAI client: {e}")
            self.client = None

    def set_few_shot_examples(self, examples: List[Dict[str, str]]):
        """
        Set few-shot examples for query generation

        Args:
            examples: List of dicts with 'document' and 'query' keys
        """
        self.few_shot_examples = examples

    def sample_and_generate_examples(
        self,
        corpus_table: str,
        columns: List[str],
        num_samples: int = 20,
        spark_session = None
    ):
        """
        Sample documents from corpus and generate example queries

        Args:
            corpus_table: Full table name (catalog.schema.table)
            columns: Column names containing text to sample
            num_samples: Number of documents to sample
            spark_session: PySpark SparkSession (auto-detected if None)

        Returns:
            DataFrame with sampled documents and generated queries
        """
        if spark_session is None:
            from pyspark.sql import SparkSession
            spark_session = SparkSession.builder.getOrCreate()

        # Sample documents
        df = spark_session.table(corpus_table)

        # Build text column
        if len(columns) == 1:
            text_col = columns[0]
        else:
            # Concatenate multiple columns
            from pyspark.sql.functions import concat_ws, col
            df = df.withColumn("_combined_text", concat_ws(" ", *[col(c) for c in columns]))
            text_col = "_combined_text"

        # Sample rows
        sample_df = df.select(text_col).limit(num_samples * 2).collect()

        # Filter out empty/short documents
        samples = []
        for row in sample_df:
            text = row[text_col]
            if text and len(str(text).strip()) > 50:
                samples.append(str(text))
                if len(samples) >= num_samples:
                    break

        # Generate queries for samples
        results = []
        for doc_text in samples[:num_samples]:
            # Truncate long documents
            if len(doc_text) > 1500:
                doc_text = doc_text[:1500] + "..."

            # Generate both keyword and natural query
            keyword_query = self._generate_query(doc_text, style="keyword")
            natural_query = self._generate_query(doc_text, style="natural")

            results.append({
                "document": doc_text[:500] + ("..." if len(doc_text) > 500 else ""),
                "keyword_query": keyword_query,
                "natural_query": natural_query
            })

        # Convert to pandas for display
        import pandas as pd
        return pd.DataFrame(results)

    def generate_queries(
        self,
        corpus_table: str,
        columns: List[str],
        num_queries: int = 200,
        style: str = "keyword",
        doc_id_column: str = "doc_id",
        spark_session = None
    ):
        """
        Generate evaluation queries from corpus

        Args:
            corpus_table: Full table name (catalog.schema.table)
            columns: Column names containing text to query
            num_queries: Number of queries to generate
            style: Query style - 'keyword', 'natural', or 'mixed'
            doc_id_column: Column name for document ID (default: 'doc_id')
            spark_session: PySpark SparkSession (auto-detected if None)

        Returns:
            PySpark DataFrame with columns: doc_id, query_text, source_text
        """
        if spark_session is None:
            from pyspark.sql import SparkSession
            spark_session = SparkSession.builder.getOrCreate()

        print(f"Generating {num_queries} {style} queries from {corpus_table}...")

        # Load corpus
        df = spark_session.table(corpus_table)

        # Build text column
        if len(columns) == 1:
            text_col = columns[0]
        else:
            # Concatenate multiple columns
            from pyspark.sql.functions import concat_ws, col
            df = df.withColumn("_combined_text", concat_ws(" ", *[col(c) for c in columns]))
            text_col = "_combined_text"

        # Sample documents (sample more than needed to account for rejections)
        sample_size = min(num_queries * 2, df.count())
        docs = df.limit(sample_size).collect()

        # Shuffle if random seed is set
        if self.random_seed is not None:
            random.shuffle(docs)

        # Generate queries
        queries = []
        for i, row in enumerate(docs):
            if len(queries) >= num_queries:
                break

            # Get document text
            doc_text = row[text_col]
            if not doc_text or len(str(doc_text).strip()) < 50:
                continue

            doc_text = str(doc_text)

            # Determine style for this query
            if style == "mixed":
                query_style = "keyword" if i % 2 == 0 else "natural"
            else:
                query_style = style

            # Generate query
            try:
                query = self._generate_query(doc_text, style=query_style)

                if query and len(query.strip()) > 3:
                    # Try to get doc_id from specified column, fall back to generic ID
                    row_dict = row.asDict()
                    doc_id = row_dict.get(doc_id_column)
                    
                    if doc_id is None:
                        doc_id = f"doc_{i}"

                    queries.append({
                        "doc_id": str(doc_id),
                        "query_text": query,
                        "source_text": doc_text[:500] + ("..." if len(doc_text) > 500 else "")
                    })

                    if (len(queries) % 10 == 0):
                        print(f"Generated {len(queries)}/{num_queries} queries...")

            except Exception as e:
                print(f"Warning: Failed to generate query for document {i}: {e}")
                continue

        print(f"Successfully generated {len(queries)} queries")

        # Convert to PySpark DataFrame
        return spark_session.createDataFrame(queries)

    def _generate_query(self, document_text: str, style: str = "keyword", max_retries: int = 3) -> str:
        """
        Generate a single query from document text

        Args:
            document_text: Source document text
            style: 'keyword' or 'natural'
            max_retries: Maximum retry attempts

        Returns:
            Generated query string
        """
        # Truncate document if too long
        if len(document_text) > 1500:
            document_text = document_text[:1500] + "..."

        # Build prompt with few-shot examples
        prompt = self._build_prompt(document_text, style)

        # Call LLM API
        for attempt in range(max_retries):
            try:
                query = self._call_llm_api(prompt)
                return query.strip()
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise

        return ""

    def _build_prompt(self, document_text: str, style: str) -> str:
        """
        Build prompt for query generation

        Args:
            document_text: Source document text
            style: 'keyword' or 'natural'

        Returns:
            Prompt string
        """
        if style == "keyword":
            style_instruction = "Generate a SHORT keyword-based search query (2-5 words) that a user would type to find this information."
            style_examples = "Examples: 'python list comprehension', 'databricks clusters cost', 'vector search indexing'"
        else:  # natural
            style_instruction = "Generate a natural language question that a user would ask to find this information."
            style_examples = "Examples: 'How do I use list comprehension in Python?', 'What is the cost of Databricks clusters?', 'How does vector search indexing work?'"

        prompt = f"""You are helping generate realistic search queries for testing a search system.

Task: Read the document below and generate a query that a user would type to find this document.

{style_instruction}

{style_examples}

Document:
{document_text}

"""

        # Add few-shot examples if available
        if self.few_shot_examples:
            prompt += "\nExamples of good document-query pairs:\n\n"
            for i, example in enumerate(self.few_shot_examples[:3], 1):
                prompt += f"Example {i}:\n"
                prompt += f"Document: {example['document'][:300]}...\n"
                prompt += f"Query: {example['query']}\n\n"

        prompt += """Now generate a query for the document above.

Rules:
- Generate ONLY the query, nothing else
- No explanations, no quotes, no extra text
- The query should be realistic - what a user would actually search for
"""

        if style == "keyword":
            prompt += "- Keep it SHORT (2-5 words)\n"
        else:
            prompt += "- Use natural language (a complete question)\n"

        prompt += "\nGenerated query:"

        return prompt

    def _call_llm_api(self, prompt: str, max_tokens: int = 50) -> str:
        """
        Call Databricks serving endpoint using OpenAI client

        Args:
            prompt: Prompt for the model
            max_tokens: Maximum tokens to generate

        Returns:
            Generated text
        """
        if not self.client:
            raise ValueError("OpenAI client not initialized")

        try:
            response = self.client.chat.completions.create(
                model=self.model_endpoint,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.7
            )

            return response.choices[0].message.content.strip()

        except Exception as e:
            raise ValueError(f"Failed to call LLM API: {e}")
