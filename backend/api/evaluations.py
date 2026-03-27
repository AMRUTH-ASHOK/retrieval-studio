"""
Evaluation jobs API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
import uuid

from backend.models.schemas import EvaluationCreate, EvaluationResponse
from backend.auth import get_workspace_client, get_sql_connector
from backend.config import settings
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.jobs import submit_eval_job, get_job_run_status, get_job_url
from utils.postgres_state import get_build as get_run

router = APIRouter()


@router.post("", response_model=EvaluationResponse, include_in_schema=False)
@router.post("/", response_model=EvaluationResponse)
async def create_evaluation(
    eval_request: EvaluationCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_sql_connector)
):
    """Submit an evaluation job to Databricks"""
    try:
        # Get build run to extract project_name
        build_run = get_run(eval_request.run_id)
        if not build_run:
            raise HTTPException(status_code=404, detail="Build run not found")
        
        project_name = build_run.get("project_name", "default")
        top_k = eval_request.top_k or 10
        dataset_type = eval_request.dataset_type or "delta_table"
        auto_generate = eval_request.auto_generate_queries or False
        build_parent_run_id = build_run.get("build_parent_run_id")
        use_golden = eval_request.use_golden_dataset or False
        generate_golden = eval_request.generate_golden_dataset or False
        
        corpus_tables_list = None

        if generate_golden and use_golden:
            raise HTTPException(status_code=400, detail="generate_golden_dataset and use_golden_dataset cannot both be true")
        if use_golden:
            if not eval_request.golden_dataset_table:
                raise HTTPException(status_code=400, detail="golden_dataset_table is required when use_golden_dataset is true")
        elif auto_generate:
            # Auto-generate golden table name if not provided
            if generate_golden and not eval_request.golden_dataset_table:
                from retrieval_core.configs import config as core_config
                eval_request.golden_dataset_table = core_config.golden_dataset_table(project_name)
                print(f"[INFO] Auto-generated golden_dataset_table: {eval_request.golden_dataset_table}")
            if not eval_request.corpus_table:
                try:
                    from backend.utils.build_results import construct_build_results, extract_corpus_tables, extract_corpus_table

                    build_config = build_run.get("config", {})
                    project_name = build_run.get("project_name")

                    if not project_name:
                        raise HTTPException(status_code=400, detail="Project name not found in build record.")

                    build_sources = build_config.get("sources", [])
                    if build_sources:
                        build_results = construct_build_results(
                            project_name=project_name,
                            strategies=[],
                            catalog=settings.CATALOG,
                            schema=settings.SCHEMA,
                            sources=build_sources
                        )
                        corpus_tables_list = extract_corpus_tables(build_results)
                        if corpus_tables_list:
                            eval_request.corpus_table = corpus_tables_list[0]["table"]
                    else:
                        strategies = list(build_config.get("strategies", {}).keys()) or ["baseline"]
                        build_results = construct_build_results(
                            project_name=project_name,
                            strategies=strategies,
                            catalog=settings.CATALOG,
                            schema=settings.SCHEMA
                        )
                        eval_request.corpus_table = extract_corpus_table(build_results)

                except Exception as e:
                    if isinstance(e, HTTPException):
                        raise
                    raise HTTPException(status_code=400, detail=f"Failed to construct corpus table: {str(e)}")
        else:
            if not eval_request.queries_table:
                raise HTTPException(status_code=400, detail="queries_table is required when auto_generate_queries is false")

        eval_id = str(uuid.uuid4())
        notebook_path = settings.EVAL_NOTEBOOK_PATH

        job_run_id = submit_eval_job(
            w=w,
            notebook_path=notebook_path,
            build_run_id=eval_request.run_id,
            eval_id=eval_id,
            build_parent_run_id=build_parent_run_id,
            queries_table=eval_request.queries_table,
            corpus_table=eval_request.corpus_table,
            corpus_tables=corpus_tables_list,
            project_name=project_name,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            dataset_type=dataset_type,
            top_k=top_k,
            auto_generate_queries=auto_generate,
            num_queries=eval_request.num_queries or 50,
            query_style=eval_request.query_style or "keyword",
            compare_query_types=eval_request.compare_query_types or False,
            judge_model_endpoint=eval_request.judge_model_endpoint,
            generate_golden_dataset=generate_golden,
            use_golden_dataset=use_golden,
            golden_dataset_table=eval_request.golden_dataset_table,
            golden_dataset_id=eval_request.golden_dataset_id,
            golden_strategy=eval_request.golden_strategy,
            golden_query_type=eval_request.golden_query_type,
            golden_top_k=eval_request.golden_top_k
        )
        
        # Job submitted successfully - now get job details (non-critical)
        job_url = None
        try:
            job_url = get_job_url(w, job_run_id)
        except Exception as e:
            print(f"Warning: Failed to get job URL: {e}")

        # Create evaluation record in Postgres
        from utils.postgres_state import create_evaluation, update_evaluation_state
        import datetime
        evaluation = create_evaluation(
            eval_id=eval_id,
            run_id=eval_request.run_id,
            project_id=build_run.get("project_id"),
            queries_table=eval_request.queries_table,
            corpus_table=eval_request.corpus_table,
            dataset_type=dataset_type,
            top_k=top_k,
            auto_generate_queries=auto_generate,
            num_queries=eval_request.num_queries,
            query_style=eval_request.query_style,
            compare_query_types=eval_request.compare_query_types,
            judge_model_endpoint=eval_request.judge_model_endpoint
        )

        # Update evaluation with job details
        evaluation = update_evaluation_state(
            eval_id=eval_id,
            state="RUNNING",
            job_run_id=job_run_id,
            job_url=job_url
        )

        # Return evaluation details
        return {
            "eval_id": eval_id,
            "run_id": eval_id,  # Frontend expects run_id for status polling
            "state": "RUNNING",
            "job_id": str(job_run_id),
            "job_url": job_url,
            "created_at": evaluation.get("created_at") if evaluation.get("created_at") else datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "updated_at": evaluation.get("updated_at") if evaluation.get("updated_at") else datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{eval_id}")
async def delete_evaluation_by_id(eval_id: str, sql_connector=Depends(get_sql_connector)):
    """Delete an evaluation"""
    try:
        from utils.postgres_state import delete_evaluation
        success = delete_evaluation(eval_id)
        if not success:
            raise HTTPException(status_code=404, detail="Evaluation not found")
        return {"success": True, "message": f"Evaluation {eval_id} deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{eval_id}/status")
async def get_evaluation_status(eval_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get evaluation job status with job URL"""
    try:
        # Get evaluation from Postgres (eval_id is actually the evaluation ID)
        from utils.postgres_state import get_evaluation
        evaluation = get_evaluation(eval_id)
        if not evaluation:
            raise HTTPException(status_code=404, detail="Evaluation not found")

        job_run_id = evaluation.get("job_run_id")
        if not job_run_id:
            return {
                "run_id": eval_id,
                "state": evaluation.get("state", "UNKNOWN"),
                "job_url": None,
                "status": None,
                "start_time": None,
            }

        # Get job status from Databricks
        job_status = get_job_run_status(w, job_run_id)
        job_url = get_job_url(w, job_run_id)

        # Update state in database if it changed
        current_state = evaluation.get("state")
        new_state = job_status.get("result_state") or job_status.get("state")

        # Map Databricks states to our states
        state_mapping = {
            "SUCCESS": "SUCCESS",
            "FAILED": "FAILED",
            "CANCELED": "FAILED",
            "TIMEDOUT": "FAILED",
            "RUNNING": "RUNNING",
            "PENDING": "PENDING",
        }
        mapped_state = state_mapping.get(new_state, current_state)

        if mapped_state != current_state:
            from utils.postgres_state import update_evaluation_state
            update_evaluation_state(
                eval_id=eval_id,
                state=mapped_state
            )

        return {
            "run_id": eval_id,
            "state": mapped_state,
            "job_url": job_url,
            "status": job_status,
            "start_time": job_status.get("start_time"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/results")
async def get_evaluation_results(run_id: str, sql_connector=Depends(get_sql_connector)):
    """Get evaluation results for a run"""
    try:
        from utils.query_builder import escape_identifier, sanitize_string

        run_id_safe = sanitize_string(run_id)

        print(f"\n[DEBUG EVAL-RESULTS] ========== START ==========")
        print(f"[DEBUG EVAL-RESULTS] Requested run_id: {repr(run_id)}")
        print(f"[DEBUG EVAL-RESULTS] Sanitized run_id: {repr(run_id_safe)}")
        print(f"[DEBUG EVAL-RESULTS] settings.CATALOG: {repr(settings.CATALOG)}")

        table_name = f"{escape_identifier(settings.CATALOG)}.{escape_identifier(settings.SCHEMA)}.rs_eval_results"
        print(f"[DEBUG EVAL-RESULTS] Table name: {table_name}")

        # Check if table exists
        try:
            check_query = f"SELECT 1 FROM {table_name} LIMIT 1"
            print(f"[DEBUG EVAL-RESULTS] Checking table existence: {check_query}")
            check_result = sql_connector.execute(check_query)
            print(f"[DEBUG EVAL-RESULTS] Table exists, check returned: {check_result}")
        except Exception as table_check_error:
            error_str = str(table_check_error).lower()
            print(f"[DEBUG EVAL-RESULTS] Table check error: {table_check_error}")
            if "table_or_view_not_found" in error_str or "table not found" in error_str or "does not exist" in error_str:
                print(f"[DEBUG EVAL-RESULTS] Table does NOT exist. Returning [].")
                return []
            raise

        # First: show what IDs exist in the table
        try:
            sample_query = f"SELECT DISTINCT eval_id, build_run_id FROM {table_name} LIMIT 20"
            print(f"[DEBUG EVAL-RESULTS] Sampling distinct IDs from table...")
            sample = sql_connector.execute(sample_query)
            print(f"[DEBUG EVAL-RESULTS] Distinct IDs in table ({len(sample)} rows):")
            for row in sample:
                print(f"[DEBUG EVAL-RESULTS]   eval_id={row.get('eval_id')}, build_run_id={row.get('build_run_id')}")
        except Exception as sample_err:
            print(f"[DEBUG EVAL-RESULTS] Failed to sample IDs: {sample_err}")

        # Now run the actual query
        query = f"""
            SELECT * FROM {table_name}
            WHERE build_run_id = '{run_id_safe}'
               OR eval_id = '{run_id_safe}'
               OR eval_run_id = '{run_id_safe}'
               OR build_child_run_id = '{run_id_safe}'
            ORDER BY created_at DESC
        """

        print(f"[DEBUG EVAL-RESULTS] Running query:")
        print(f"[DEBUG EVAL-RESULTS]   {query.strip()}")
        results = sql_connector.execute(query)
        print(f"[DEBUG EVAL-RESULTS] Query returned {len(results) if results else 0} rows")

        if results and len(results) > 0:
            print(f"[DEBUG EVAL-RESULTS] First row keys: {list(results[0].keys())}")
            print(f"[DEBUG EVAL-RESULTS] First row eval_id: {results[0].get('eval_id')}")
            print(f"[DEBUG EVAL-RESULTS] First row build_run_id: {results[0].get('build_run_id')}")
        else:
            print(f"[DEBUG EVAL-RESULTS] NO ROWS MATCHED. The run_id '{run_id_safe}' was not found in any column.")
            print(f"[DEBUG EVAL-RESULTS] This means the eval_id passed by the frontend does not match any eval_id/build_run_id/eval_run_id/build_child_run_id in the table.")

        print(f"[DEBUG EVAL-RESULTS] ========== END ==========\n")
        return results if results else []

    except Exception as e:
        print(f"[ERROR EVAL-RESULTS] Exception: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to retrieve evaluation results: {str(e)}")


@router.post("/explain")
async def explain_strategy_comparison(request: dict):
    """Use LLM to explain why one strategy outperformed another for a data source"""
    try:
        source_name = request.get("source_name", "")
        source_type = request.get("source_type", "")
        strategies = request.get("strategies", [])
        judge_endpoint = request.get("judge_endpoint", "databricks-claude-sonnet-4-5")

        if not strategies or len(strategies) < 2:
            return {"explanation": "Need at least 2 strategies to compare."}

        metrics_table = "\n".join([
            f"- {s.get('strategy_name', '?')}: Recall@10={s.get('recall_at_10', 0):.3f}, "
            f"NDCG@10={s.get('ndcg_at_10', 0):.3f}, "
            f"Precision@10={s.get('precision_at_10', 0):.3f}, "
            f"Latency={s.get('avg_latency_ms', 0):.0f}ms"
            for s in strategies
        ])

        best = max(strategies, key=lambda s: s.get("recall_at_10", 0))
        best_name = best.get("strategy_name", "unknown")

        prompt = (
            f"Compare these chunking strategies for {source_type} data source \"{source_name}\":\n\n"
            f"{metrics_table}\n\n"
            f"The best performing strategy is \"{best_name}\".\n\n"
            f"In 2-3 concise sentences, explain:\n"
            f"1. Why {best_name} performs best for this type of data\n"
            f"2. Key trade-offs between the strategies (quality vs latency)\n"
            f"3. A practical recommendation"
        )

        try:
            from openai import OpenAI
            client = OpenAI(
                base_url=os.environ.get("DATABRICKS_HOST", "") + "/serving-endpoints",
                api_key=os.environ.get("DATABRICKS_TOKEN", "")
            )
            response = client.chat.completions.create(
                model=judge_endpoint,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.3
            )
            explanation = response.choices[0].message.content
        except Exception as llm_err:
            explanation = (
                f"{best_name} achieved the highest Recall@10 "
                f"({best.get('recall_at_10', 0):.3f}) for the {source_type} data source "
                f"\"{source_name}\". This suggests its chunking approach better preserves "
                f"the semantic boundaries relevant to this content type."
            )

        return {"explanation": explanation}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/build/{build_run_id}", response_model=List[EvaluationResponse])
async def get_evaluations_by_build(build_run_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get all evaluations for a specific build run"""
    try:
        from utils.postgres_state import get_evaluations_by_build

        evaluations = get_evaluations_by_build(build_run_id)

        # Transform to EvaluationResponse format
        transformed_evals = []
        for eval_data in evaluations:
            job_run_id = eval_data.get("job_run_id")
            job_url = None

            # Generate job URL if job_run_id exists
            if job_run_id:
                try:
                    job_url = get_job_url(w, job_run_id)
                except Exception:
                    pass  # If URL generation fails, continue without it

            transformed_evals.append({
                "eval_id": eval_data.get("eval_id"),
                "run_id": eval_data.get("eval_id"),  # Frontend expects run_id
                "state": eval_data.get("state", "UNKNOWN"),
                "job_id": str(job_run_id) if job_run_id else None,
                "job_url": job_url,
                "created_at": eval_data.get("created_at"),
                "updated_at": eval_data.get("updated_at"),
            })

        return transformed_evals
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
