#!/usr/bin/env python3
"""DAG importability and dependency tests."""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Add dags directory to path
dags_dir = os.path.join(project_root, 'dags')
sys.path.insert(0, dags_dir)


def test_dag_imports():
    """Test that DAGs can be imported without errors."""
    print("Testing DAG imports...")
    
    # Test full_load DAG
    try:
        import articles_full_load
        dag = articles_full_load.dag
        assert dag.dag_id == 'articles_full_load'
        assert dag.schedule_interval is None  # Manual trigger only
        assert dag.max_active_runs == 1
        print("  ✓ articles_full_load imported successfully")
    except Exception as e:
        print(f"  ✗ articles_full_load import failed: {e}")
        return False
    
    # Test incremental_load DAG
    try:
        import articles_incremental_load
        dag = articles_incremental_load.dag
        assert dag.dag_id == 'articles_incremental_load'
        assert dag.schedule_interval == '0 * * * *'  # Hourly
        assert dag.max_active_runs == 1
        print("  ✓ articles_incremental_load imported successfully")
    except Exception as e:
        print(f"  ✗ articles_incremental_load import failed: {e}")
        return False
    
    return True


def test_task_dependencies():
    """Test that task dependencies are correctly defined."""
    print("Testing task dependencies...")
    
    import articles_full_load
    import articles_incremental_load
    
    # Test full_load DAG dependencies
    dag_full = articles_full_load.dag
    task_ids = {t.task_id for t in dag_full.tasks}
    expected_tasks = {'check_pipeline_state', 'run_full_load', 'run_gold_transformation', 'verify_pipeline_ready'}
    assert task_ids == expected_tasks, f"Full load tasks mismatch: {task_ids}"
    
    # Check dependency chain: check_state >> full_load >> gold_transform >> verify_ready
    check_state = dag_full.get_task('check_pipeline_state')
    full_load = dag_full.get_task('run_full_load')
    gold_transform = dag_full.get_task('run_gold_transformation')
    verify_ready = dag_full.get_task('verify_pipeline_ready')
    
    assert full_load in check_state.downstream_list, "check_pipeline_state >> run_full_load"
    assert gold_transform in full_load.downstream_list, "run_full_load >> run_gold_transformation"
    assert verify_ready in gold_transform.downstream_list, "run_gold_transformation >> verify_pipeline_ready"
    print("  ✓ Full load DAG dependencies correct")
    
    # Test incremental_load DAG dependencies
    dag_inc = articles_incremental_load.dag
    task_ids_inc = {t.task_id for t in dag_inc.tasks}
    expected_tasks_inc = {'check_pipeline_ready', 'run_incremental_load', 'run_gold_transformation'}
    assert task_ids_inc == expected_tasks_inc, f"Incremental tasks mismatch: {task_ids_inc}"
    
    check_state_inc = dag_inc.get_task('check_pipeline_ready')
    incremental_load = dag_inc.get_task('run_incremental_load')
    gold_transform_inc = dag_inc.get_task('run_gold_transformation')
    
    assert incremental_load in check_state_inc.downstream_list, "check_pipeline_ready >> run_incremental_load"
    assert gold_transform_inc in incremental_load.downstream_list, "run_incremental_load >> run_gold_transformation"
    print("  ✓ Incremental load DAG dependencies correct")
    
    return True


def test_schedule_intervals():
    """Test that schedule intervals are correctly set."""
    print("Testing schedule intervals...")
    
    import articles_full_load
    import articles_incremental_load
    
    # Full load should be manual (None)
    assert articles_full_load.dag.schedule_interval is None, "Full load should be manual trigger only"
    
    # Incremental should be hourly
    assert articles_incremental_load.dag.schedule_interval == '0 * * * *', "Incremental should be hourly"
    
    print("  ✓ Schedule intervals correct")
    return True


def test_max_active_runs():
    """Test that max_active_runs is set to 1 for both DAGs."""
    print("Testing max_active_runs...")
    
    import articles_full_load
    import articles_incremental_load
    
    assert articles_full_load.dag.max_active_runs == 1, "Full load max_active_runs should be 1"
    assert articles_incremental_load.dag.max_active_runs == 1, "Incremental max_active_runs should be 1"
    
    print("  ✓ max_active_runs correct")
    return True


def test_task_timeouts():
    """Test that tasks have appropriate timeouts."""
    print("Testing task timeouts...")
    
    import articles_full_load
    import articles_incremental_load
    
    # Full load tasks should have execution_timeout
    full_load_task = articles_full_load.dag.get_task('run_full_load')
    assert full_load_task.execution_timeout is not None, "Full load task should have execution_timeout"
    assert full_load_task.execution_timeout.total_seconds() >= 7200, "Full load timeout should be >= 2 hours"
    
    # Incremental load task should have execution_timeout
    inc_task = articles_incremental_load.dag.get_task('run_incremental_load')
    assert inc_task.execution_timeout is not None, "Incremental task should have execution_timeout"
    assert inc_task.execution_timeout.total_seconds() >= 2700, "Incremental timeout should be >= 45 minutes"
    
    print("  ✓ Task timeouts correct")
    return True


def test_retry_configuration():
    """Test retry configuration."""
    print("Testing retry configuration...")
    
    import articles_full_load
    import articles_incremental_load
    
    # Check retries in default_args
    full_args = articles_full_load.default_args
    inc_args = articles_incremental_load.default_args
    
    assert full_args['retries'] == 0, "Full load retries should be 0 (immediate failure)"
    assert inc_args['retries'] == 0, "Incremental retries should be 0 (immediate failure)"
    
    # When retries=0, retry_delay may not be set - that's fine for immediate failure
    print("  ✓ Retry configuration correct")
    return True


def test_concurrency_protection():
    """Test that concurrency protection mechanisms are in place."""
    print("Testing concurrency protection...")
    
    import articles_full_load
    import articles_incremental_load
    
    # Both DAGs should have max_active_runs = 1
    assert articles_full_load.dag.max_active_runs == 1
    assert articles_incremental_load.dag.max_active_runs == 1
    
    # Check that incremental has check_pipeline_ready task
    inc_dag = articles_incremental_load.dag
    check_task = inc_dag.get_task('check_pipeline_ready')
    assert check_task is not None, "Incremental should have check_pipeline_ready task"
    
    print("  ✓ Concurrency protection present")
    return True


def run_all_tests():
    """Run all DAG tests."""
    print("=" * 60)
    print("Phase 6: DAG Importability and Dependency Tests")
    print("=" * 60)
    
    tests = [
        test_dag_imports,
        test_task_dependencies,
        test_schedule_intervals,
        test_max_active_runs,
        test_task_timeouts,
        test_retry_configuration,
        test_concurrency_protection,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__} failed with exception: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)