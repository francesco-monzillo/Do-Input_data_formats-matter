import pandas as pd
from sklearn.metrics import cohen_kappa_score
import numpy as np
from statsmodels.stats.inter_rater import fleiss_kappa, aggregate_raters
from itertools import combinations


def calculate_two_annotator_agreement(csv_file_path, annotator1_col='Annotator1', annotator2_col='Annotator2'):
    """
    Calculate detailed inter-annotator agreement for exactly 2 annotators.
    Provides confusion matrices and detailed disagreement analysis.
    
    Parameters:
    -----------
    csv_file_path : str
        Path to the directory containing TSV files
    annotator1_col : str
        Name of first annotator column
    annotator2_col : str
        Name of second annotator column
    """
    llm_considered = csv_file_path.split("/")[-1]
    
    # Load CSV files
    tasks = {
        'CSV data|JSON doc': pd.read_csv(f"{csv_file_path}/csv_data_json_doc.tsv", sep="\t"),
        'KG data|Embedded doc': pd.read_csv(f"{csv_file_path}/kg_data_embedded_doc.tsv", sep="\t"),
        'Unstructured data|Unstructured doc': pd.read_csv(f"{csv_file_path}/unstructured_data_unstructured_doc.tsv", sep="\t")
    }

    print("=" * 70)
    print(f"TWO-ANNOTATOR AGREEMENT ANALYSIS FOR {llm_considered}")
    print(f"Comparing: {annotator1_col} vs {annotator2_col}")
    print("=" * 70)

    # Store results for summary
    results = []

    # Calculate agreement for each task
    for task_name, df in tasks.items():
        print(f"\n{'='*70}")
        print(f"{task_name.upper()} (n={len(df)} samples)")
        print("="*70)
        
        ann1_numeric = df[annotator1_col].values
        ann2_numeric = df[annotator2_col].values
        
        ann1_str = ann1_numeric.astype(str)
        ann2_str = ann2_numeric.astype(str)
        
        def map_to_ordinal(arr):
            result = np.zeros(len(arr), dtype=int)
            result[arr == 0] = 0
            result[arr == 0.5] = 1
            result[arr == 1.0] = 2
            return result
        
        ann1_ordinal = map_to_ordinal(ann1_numeric)
        ann2_ordinal = map_to_ordinal(ann2_numeric)
        
        # Percent agreement
        pct_agree = np.sum(ann1_numeric == ann2_numeric) / len(ann1_numeric)
        
        # Cohen's Kappa
        kappa = cohen_kappa_score(ann1_str, ann2_str)
        
        # Weighted Kappa
        weighted_kappa_linear = cohen_kappa_score(ann1_ordinal, ann2_ordinal, weights='linear')
        weighted_kappa_quad = cohen_kappa_score(ann1_ordinal, ann2_ordinal, weights='quadratic')
        
        print(f"\nAgreement Metrics:")
        print(f"  Percent Agreement:        {pct_agree:.3f} ({pct_agree*100:.1f}%)")
        print(f"  Cohen's Kappa:            {kappa:.3f}")
        print(f"  Weighted Kappa (linear):  {weighted_kappa_linear:.3f}")
        print(f"  Weighted Kappa (quad):    {weighted_kappa_quad:.3f}")
        
        # Confusion Matrix 
        cm = np.zeros((3, 3), dtype=int)
        for a1, a2 in zip(ann1_numeric, ann2_numeric):
            i = 0 if a1 == 0 else (1 if a1 == 0.5 else 2)
            j = 0 if a2 == 0 else (1 if a2 == 0.5 else 2)
            cm[i, j] += 1
        
        print(f"\nConfusion Matrix:")
        print(f"                 {annotator2_col}")
        print(f"                 0      0.5    1")
        print(f"{annotator1_col:12s} 0   {cm[0][0]:3d}    {cm[0][1]:3d}    {cm[0][2]:3d}")
        print(f"             0.5 {cm[1][0]:3d}    {cm[1][1]:3d}    {cm[1][2]:3d}")
        print(f"             1   {cm[2][0]:3d}    {cm[2][1]:3d}    {cm[2][2]:3d}")
        
        # Rating Distribution
        print(f"\nRating Distribution:")
        print(f"  {annotator1_col}: 0={np.sum(ann1_numeric==0):3d}, 0.5={np.sum(ann1_numeric==0.5):3d}, 1={np.sum(ann1_numeric==1):3d}")
        print(f"  {annotator2_col}: 0={np.sum(ann2_numeric==0):3d}, 0.5={np.sum(ann2_numeric==0.5):3d}, 1={np.sum(ann2_numeric==1):3d}")
        
        # Disagreement breakdown
        exact_match = np.sum(ann1_numeric == ann2_numeric)
        off_by_half = np.sum(np.abs(ann1_numeric - ann2_numeric) == 0.5)
        off_by_one = np.sum(np.abs(ann1_numeric - ann2_numeric) == 1.0)
        
        print(f"\nDisagreement Breakdown:")
        print(f"  Exact agreement:     {exact_match:3d} ({exact_match/len(ann1_numeric)*100:.1f}%)")
        print(f"  Off by 0.5:          {off_by_half:3d} ({off_by_half/len(ann1_numeric)*100:.1f}%)")
        print(f"  Off by 1.0:          {off_by_one:3d} ({off_by_one/len(ann1_numeric)*100:.1f}%)")
        
        results.append({
            'Task': task_name,
            'N': len(df),
            'Percent Agreement': f"{pct_agree:.3f}",
            'Kappa': f"{kappa:.3f}",
            'Weighted Kappa (linear)': f"{weighted_kappa_linear:.3f}",
            'Weighted Kappa (quad)': f"{weighted_kappa_quad:.3f}"
        })
    
    # Overall agreement
    print("\n" + "=" * 70)
    print("OVERALL AGREEMENT (ALL TASKS COMBINED)")
    print("=" * 70)
    
    all_ann1_numeric = np.concatenate([df[annotator1_col].values for df in tasks.values()])
    all_ann2_numeric = np.concatenate([df[annotator2_col].values for df in tasks.values()])
    
    all_ann1_str = all_ann1_numeric.astype(str)
    all_ann2_str = all_ann2_numeric.astype(str)
    
    all_ann1_ordinal = map_to_ordinal(all_ann1_numeric)
    all_ann2_ordinal = map_to_ordinal(all_ann2_numeric)
    
    overall_pct = np.sum(all_ann1_numeric == all_ann2_numeric) / len(all_ann1_numeric)
    overall_kappa = cohen_kappa_score(all_ann1_str, all_ann2_str)
    overall_weighted_linear = cohen_kappa_score(all_ann1_ordinal, all_ann2_ordinal, weights='linear')
    overall_weighted_quad = cohen_kappa_score(all_ann1_ordinal, all_ann2_ordinal, weights='quadratic')
    
    print(f"\nTotal samples: {len(all_ann1_numeric)}")
    print(f"\nAgreement Metrics:")
    print(f"  Percent Agreement:        {overall_pct:.3f} ({overall_pct*100:.1f}%)")
    print(f"  Cohen's Kappa:            {overall_kappa:.3f}")
    print(f"  Weighted Kappa (linear):  {overall_weighted_linear:.3f}")
    print(f"  Weighted Kappa (quad):    {overall_weighted_quad:.3f}")
    
    # Overall Confusion Matrix
    cm_overall = np.zeros((3, 3), dtype=int)
    for a1, a2 in zip(all_ann1_numeric, all_ann2_numeric):
        i = 0 if a1 == 0 else (1 if a1 == 0.5 else 2)
        j = 0 if a2 == 0 else (1 if a2 == 0.5 else 2)
        cm_overall[i, j] += 1
    
    print(f"\nOverall Confusion Matrix:")
    print(f"                 {annotator2_col}")
    print(f"                 0      0.5    1")
    print(f"{annotator1_col:12s} 0   {cm_overall[0][0]:3d}    {cm_overall[0][1]:3d}    {cm_overall[0][2]:3d}")
    print(f"             0.5 {cm_overall[1][0]:3d}    {cm_overall[1][1]:3d}    {cm_overall[1][2]:3d}")
    print(f"             1   {cm_overall[2][0]:3d}    {cm_overall[2][1]:3d}    {cm_overall[2][2]:3d}")
    
    # Overall disagreement breakdown
    overall_exact = np.sum(all_ann1_numeric == all_ann2_numeric)
    overall_half = np.sum(np.abs(all_ann1_numeric - all_ann2_numeric) == 0.5)
    overall_one = np.sum(np.abs(all_ann1_numeric - all_ann2_numeric) == 1.0)
    
    print(f"\nOverall Disagreement Breakdown:")
    print(f"  Exact agreement:     {overall_exact:3d} ({overall_exact/len(all_ann1_numeric)*100:.1f}%)")
    print(f"  Off by 0.5:          {overall_half:3d} ({overall_half/len(all_ann1_numeric)*100:.1f}%)")
    print(f"  Off by 1.0:          {overall_one:3d} ({overall_one/len(all_ann1_numeric)*100:.1f}%)")
    
    # Summary table
    print("\n" + "=" * 70)
    print("SUMMARY TABLE")
    print("=" * 70)
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    print("\n" + "=" * 70)
    print("INTERPRETATION")
    print("=" * 70)
    
    def interpret_kappa(k):
        if k < 0: return "No agreement"
        elif k < 0.21: return "Slight agreement"
        elif k < 0.41: return "Fair agreement"
        elif k < 0.61: return "Moderate agreement"
        elif k < 0.81: return "Substantial agreement"
        else: return "Almost perfect agreement"
    
    print(f"\nOverall Agreement Level: {interpret_kappa(overall_weighted_linear)}")
    print("\nKappa Interpretation Scale (Landis & Koch, 1977):")
    print("  < 0.00:     No agreement")
    print("  0.00-0.20:  Slight agreement")
    print("  0.21-0.40:  Fair agreement")
    print("  0.41-0.60:  Moderate agreement")
    print("  0.61-0.80:  Substantial agreement")
    print("  0.81-1.00:  Almost perfect agreement")
    print("=" * 70)


def calculate_multi_annotator_agreement(csv_file_path, annotator_cols=None):
    """
    Calculate inter-annotator agreement for 3 or more annotators.
    
    Parameters:
    -----------
    csv_file_path : str
        Path to the directory containing TSV files
    annotator_cols : list, optional
        List of annotator column names. If None, auto-detects columns starting with 'Annotator'
    """
    llm_considered = csv_file_path.split("/")[-1]
    
    # Load CSV files
    tasks = {
        'CSV data|JSON doc': pd.read_csv(f"{csv_file_path}/csv_data_json_doc.csv", sep=","),
        'KG data|Embedded doc': pd.read_csv(f"{csv_file_path}/kg_data_embedded_doc.csv", sep=","),
        'Unstructured data|Unstructured doc': pd.read_csv(f"{csv_file_path}/unstructured_data_unstructured_doc.csv", sep=","),
        'Time labeled hypergraph' : pd.read_csv(f"{csv_file_path}/time_labeled_hyp.csv", sep=","),
        'Metric labeled hypergraph' : pd.read_csv(f"{csv_file_path}/metric_labeled_hyp.csv", sep=","),
        'KG labeled hypergraph' : pd.read_csv(f"{csv_file_path}/kg_labeled_hyp.csv", sep=","),
        'Value labeled hypergraph' : pd.read_csv(f"{csv_file_path}/value_labeled_hyp.csv", sep=","),
        '4-uniform hypergraph' : pd.read_csv(f"{csv_file_path}/4_uniform_hyp.csv", sep=",")
    }

    # Auto-detect annotator columns if not provided
    if annotator_cols is None:
        sample_df = list(tasks.values())[0]
        annotator_cols = [col for col in sample_df.columns if col.startswith('Annotator')]
        annotator_cols.sort()  # Ensure consistent ordering
    
    n_annotators = len(annotator_cols)
    
    print("=" * 70)
    print(f"MULTI-ANNOTATOR AGREEMENT ANALYSIS FOR {llm_considered}")
    print(f"Number of annotators: {n_annotators}")
    print(f"Annotators: {', '.join(annotator_cols)}")
    print("=" * 70)

    # Store results for summary
    results = []
    
    # Store per-task metrics for computing std across tasks
    all_fleiss_kappas = []
    all_kripp_alphas = []
    all_avg_kappas = []
    all_avg_pcts = []

    # Calculate agreement for each task
    for task_name, df in tasks.items():
        print(f"\n{'='*70}")
        print(f"{task_name.upper()} (n={len(df)} samples)")
        print("="*70)
        
        # Get ratings matrix (samples × annotators)
        ratings_matrix = df[annotator_cols].values
        
        # Calculate Fleiss' Kappa
        try:
            # Aggregate raters for Fleiss' Kappa calculation
            table, categories = aggregate_raters(ratings_matrix)
            fleiss_k = fleiss_kappa(table, method='fleiss')
        except Exception as e:
            print(f"Warning: Could not calculate Fleiss' Kappa: {e}")
            fleiss_k = np.nan

        # Calculate Krippendorff's Alpha
        kripp_alpha = calculate_krippendorffs_alpha(ratings_matrix)
        
        # Calculate pairwise agreements (all pairs of annotators)
        pairwise_results = calculate_pairwise_agreements(df, annotator_cols)
        
        print(f"\nMulti-Rater Agreement Metrics:")
        print(f"  Fleiss' Kappa:              {fleiss_k:.3f}")
        print(f"  Krippendorff's Alpha:       {kripp_alpha:.3f}")
        
        print(f"\nPairwise Agreements (Cohen's Kappa):")
        for pair, metrics in pairwise_results.items():
            print(f"  {pair}: κ={metrics['kappa']:.3f}, agreement={metrics['pct_agree']:.1f}%")
        
        # Extract pairwise kappas and percent agreements for std calculation
        pairwise_kappas = [m['kappa'] for m in pairwise_results.values()]
        pairwise_pcts = [m['pct_agree'] for m in pairwise_results.values()]
        
        # Average and std for pairwise metrics
        avg_kappa = np.mean(pairwise_kappas)
        std_kappa = np.std(pairwise_kappas, ddof=1) if len(pairwise_kappas) > 1 else 0.0
        avg_pct = np.mean(pairwise_pcts)
        std_pct = np.std(pairwise_pcts, ddof=1) if len(pairwise_pcts) > 1 else 0.0
        
        print(f"\nAverage Pairwise Metrics:")
        print(f"  Average Cohen's Kappa:      {avg_kappa:.3f} ± {std_kappa:.3f} (std)")
        print(f"  Average Percent Agreement:  {avg_pct:.1f}% ± {std_pct:.1f}% (std)")
        
        # Store for overall std calculation
        all_fleiss_kappas.append(fleiss_k)
        all_kripp_alphas.append(kripp_alpha)
        all_avg_kappas.append(avg_kappa)
        all_avg_pcts.append(avg_pct)
        
        # Rating distribution per annotator
        print(f"\nRating Distribution by Annotator:")
        for col in annotator_cols:
            ratings = df[col].values
            dist = f"0={np.sum(ratings==0):3d}, 0.5={np.sum(ratings==0.5):3d}, 1={np.sum(ratings==1):3d}"
            print(f"  {col}: {dist}")
        
        # Complete agreement analysis
        complete_agreement = calculate_complete_agreement(ratings_matrix)
        print(f"\nComplete Agreement (all annotators agree):")
        print(f"  Exact agreement on all items: {complete_agreement['exact']:3d} ({complete_agreement['exact_pct']:.1f}%)")
        print(f"  Majority agreement:          {complete_agreement['majority']:3d} ({complete_agreement['majority_pct']:.1f}%)")
        
        # Store results
        results.append({
            'Task': task_name,
            'N': len(df),
            'Fleiss Kappa': f"{fleiss_k:.3f}",
            'Krippendorff Alpha': f"{kripp_alpha:.3f}",
            'Avg Pairwise Kappa': f"{avg_kappa:.3f}",
            'Std Pairwise Kappa': f"{std_kappa:.3f}",
            'Avg Pct Agreement': f"{avg_pct:.1f}%",
            'Std Pct Agreement': f"{std_pct:.1f}%"
        })
    
    # Overall agreement across all tasks
    print("\n" + "=" * 70)
    print("OVERALL AGREEMENT (ALL TASKS COMBINED)")
    print("=" * 70)
    
    # Concatenate all ratings
    all_ratings_list = []
    for df in tasks.values():
        all_ratings_list.append(df[annotator_cols].values)
    all_ratings_matrix = np.vstack(all_ratings_list)
    
    # Overall Fleiss' Kappa
    try:
        table_overall, categories_overall = aggregate_raters(all_ratings_matrix)
        overall_fleiss = fleiss_kappa(table_overall, method='fleiss')
    except Exception as e:
        print(f"Warning: Could not calculate overall Fleiss' Kappa: {e}")
        overall_fleiss = np.nan
    
    # Overall Krippendorff's Alpha
    overall_kripp = calculate_krippendorffs_alpha(all_ratings_matrix)
    
    # Overall pairwise
    all_df = pd.concat([df[annotator_cols] for df in tasks.values()], ignore_index=True)
    overall_pairwise = calculate_pairwise_agreements(all_df, annotator_cols)
    
    # Extract pairwise metrics for overall std
    overall_pairwise_kappas = [m['kappa'] for m in overall_pairwise.values()]
    overall_pairwise_pcts = [m['pct_agree'] for m in overall_pairwise.values()]
    
    overall_avg_kappa = np.mean(overall_pairwise_kappas)
    overall_std_kappa = np.std(overall_pairwise_kappas, ddof=1) if len(overall_pairwise_kappas) > 1 else 0.0
    overall_avg_pct = np.mean(overall_pairwise_pcts)
    overall_std_pct = np.std(overall_pairwise_pcts, ddof=1) if len(overall_pairwise_pcts) > 1 else 0.0
    
    # Calculate std across tasks
    std_fleiss_across_tasks = np.std([k for k in all_fleiss_kappas if not np.isnan(k)], ddof=1) if len([k for k in all_fleiss_kappas if not np.isnan(k)]) > 1 else 0.0
    std_kripp_across_tasks = np.std([k for k in all_kripp_alphas if not np.isnan(k)], ddof=1) if len([k for k in all_kripp_alphas if not np.isnan(k)]) > 1 else 0.0
    std_avg_kappa_across_tasks = np.std(all_avg_kappas, ddof=1) if len(all_avg_kappas) > 1 else 0.0
    std_avg_pct_across_tasks = np.std(all_avg_pcts, ddof=1) if len(all_avg_pcts) > 1 else 0.0
    
    print(f"\nTotal samples: {len(all_ratings_matrix)}")
    print(f"\nOverall Agreement Metrics:")
    print(f"  Fleiss' Kappa:              {overall_fleiss:.3f}")
    print(f"  Krippendorff's Alpha:       {overall_kripp:.3f}")
    print(f"  Average Pairwise Kappa:     {overall_avg_kappa:.3f} ± {overall_std_kappa:.3f} (std across pairs)")
    print(f"  Average Percent Agreement:  {overall_avg_pct:.1f}% ± {overall_std_pct:.1f}% (std across pairs)")
    
    print(f"\nVariability Across Tasks:")
    print(f"  Fleiss' Kappa (mean ± std):           {np.nanmean(all_fleiss_kappas):.3f} ± {std_fleiss_across_tasks:.3f}")
    print(f"  Krippendorff's Alpha (mean ± std):    {np.nanmean(all_kripp_alphas):.3f} ± {std_kripp_across_tasks:.3f}")
    print(f"  Avg Pairwise Kappa (mean ± std):      {np.mean(all_avg_kappas):.3f} ± {std_avg_kappa_across_tasks:.3f}")
    print(f"  Avg Pct Agreement (mean ± std):       {np.mean(all_avg_pcts):.1f}% ± {std_avg_pct_across_tasks:.1f}%")
    
    # Overall complete agreement
    overall_complete = calculate_complete_agreement(all_ratings_matrix)
    print(f"\nOverall Complete Agreement:")
    print(f"  Exact agreement:    {overall_complete['exact']:3d} ({overall_complete['exact_pct']:.1f}%)")
    print(f"  Majority agreement: {overall_complete['majority']:3d} ({overall_complete['majority_pct']:.1f}%)")
    
    # Summary table
    print("\n" + "=" * 70)
    print("SUMMARY TABLE")
    print("=" * 70)
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    results_df.to_csv(f"{csv_file_path}/annotator_agreement_summary.csv", index=False)
    
    # Interpretation
    print("\n" + "=" * 70)
    print("INTERPRETATION")
    print("=" * 70)
    
    def interpret_kappa(k):
        if k < 0: return "No agreement"
        elif k < 0.21: return "Slight agreement"
        elif k < 0.41: return "Fair agreement"
        elif k < 0.61: return "Moderate agreement"
        elif k < 0.81: return "Substantial agreement"
        else: return "Almost perfect agreement"
    
    print(f"\nOverall Agreement Level (Fleiss' Kappa): {interpret_kappa(overall_fleiss)}")
    print(f"Overall Agreement Level (Krippendorff's Alpha): {interpret_kappa(overall_kripp)}")
    print("\nKappa Interpretation Scale (Landis & Koch, 1977):")
    print("  < 0.00:     No agreement")
    print("  0.00-0.20:  Slight agreement")
    print("  0.21-0.40:  Fair agreement")
    print("  0.41-0.60:  Moderate agreement")
    print("  0.61-0.80:  Substantial agreement")
    print("  0.81-1.00:  Almost perfect agreement")
    print("\n" + "=" * 70)
    print("NOTES:")
    print("  - Fleiss' Kappa: Measures agreement among multiple raters")
    print("  - Krippendorff's Alpha: More robust, handles ordinal data well")
    print("  - Use Fleiss' Kappa when all raters rate all items")
    print("  - Use Krippendorff's Alpha when dealing with missing ratings")
    print("  - Std (across pairs): Variability among annotator pairs within a task/overall")
    print("  - Std (across tasks): Variability of agreement metrics across different tasks")
    print("=" * 70)


def calculate_pairwise_agreements(df, annotator_cols):
    """Calculate Cohen's Kappa for all pairs of annotators."""
    pairwise_results = {}
    
    for ann1, ann2 in combinations(annotator_cols, 2):
        ratings1 = df[ann1].values
        ratings2 = df[ann2].values
        
        # Percent agreement
        pct_agree = np.sum(ratings1 == ratings2) / len(ratings1) * 100
        
        # Cohen's Kappa
        kappa = cohen_kappa_score(ratings1.astype(str), ratings2.astype(str))
        
        pair_name = f"{ann1} vs {ann2}"
        pairwise_results[pair_name] = {
            'kappa': kappa,
            'pct_agree': pct_agree
        }
    
    return pairwise_results


def calculate_complete_agreement(ratings_matrix):
    """
    Calculate complete agreement metrics.
    
    Parameters:
    -----------
    ratings_matrix : numpy array
        Shape (n_samples, n_annotators)
    """
    n_samples = ratings_matrix.shape[0]
    
    # Exact agreement (all annotators give same rating)
    exact_agreement = 0
    for row in ratings_matrix:
        if len(np.unique(row)) == 1:
            exact_agreement += 1
    
    # Majority agreement (mode appears more than half the time)
    majority_agreement = 0
    for row in ratings_matrix:
        values, counts = np.unique(row, return_counts=True)
        max_count = np.max(counts)
        if max_count > len(row) / 2:
            majority_agreement += 1
    
    return {
        'exact': exact_agreement,
        'exact_pct': exact_agreement / n_samples * 100,
        'majority': majority_agreement,
        'majority_pct': majority_agreement / n_samples * 100
    }


def calculate_krippendorffs_alpha(ratings_matrix, metric='ordinal'):
    """
    Calculate Krippendorff's Alpha for ordinal data.
    
    Parameters:
    -----------
    ratings_matrix : numpy array
        Shape (n_samples, n_annotators)
    metric : str
        'nominal', 'ordinal', 'interval', or 'ratio'
    """
    # Map ratings to ordinal values
    def map_to_ordinal(val):
        if val == 0:
            return 0
        elif val == 0.5:
            return 1
        elif val == 1.0:
            return 2
        else:
            return val
    
    # Convert to ordinal
    ordinal_matrix = np.vectorize(map_to_ordinal)(ratings_matrix)
    
    # Transpose for Krippendorff's Alpha (expects raters × items)
    reliability_data = ordinal_matrix.T
    
    n_raters, n_items = reliability_data.shape
    
    # Calculate observed disagreement
    pairable_values = []
    for item_idx in range(n_items):
        values = reliability_data[:, item_idx]
        # Get all pairs of values for this item
        for i in range(len(values)):
            for j in range(i + 1, len(values)):
                pairable_values.append((values[i], values[j]))
    
    if len(pairable_values) == 0:
        return np.nan
    
    # Calculate disagreement for ordinal metric
    def ordinal_disagreement(v1, v2):
        if v1 == v2:
            return 0
        else:
            # Squared distance for ordinal
            return (v1 - v2) ** 2
    
    observed_disagreement = np.mean([ordinal_disagreement(v1, v2) for v1, v2 in pairable_values])
    
    # Calculate expected disagreement
    all_values = reliability_data.flatten()
    expected_pairs = []
    for i in range(len(all_values)):
        for j in range(i + 1, len(all_values)):
            expected_pairs.append((all_values[i], all_values[j]))
    
    if len(expected_pairs) == 0:
        return np.nan
    
    expected_disagreement = np.mean([ordinal_disagreement(v1, v2) for v1, v2 in expected_pairs])
    
    # Krippendorff's Alpha
    if expected_disagreement == 0:
        return 1.0 if observed_disagreement == 0 else np.nan
    
    alpha = 1 - (observed_disagreement / expected_disagreement)
    
    return alpha


def auto_calculate_agreement(csv_file_path, annotator_cols=None):
    """
    Automatically detects number of annotators and runs appropriate analysis.
    
    Parameters:
    -----------
    csv_file_path : str
        Path to the directory containing TSV files
    annotator_cols : list, optional
        List of annotator column names. If None, auto-detects columns starting with 'Annotator'
    """
    # Load a sample file to detect annotators
    sample_df = pd.read_csv(f"{csv_file_path}/4_uniform_hyp.csv", sep=",")
    
    # Auto-detect annotator columns if not provided
    if annotator_cols is None:
        annotator_cols = [col for col in sample_df.columns if col.startswith('Annotator')]
        annotator_cols.sort()
    
    n_annotators = len(annotator_cols)
    
    print(f"\nDetected {n_annotators} annotators: {', '.join(annotator_cols)}\n")
    
    if n_annotators == 2:
        print("Using TWO-ANNOTATOR analysis (detailed pairwise with confusion matrices)\n")
        calculate_two_annotator_agreement(csv_file_path, annotator_cols[0], annotator_cols[1])
    elif n_annotators > 2:
        print("Using MULTI-ANNOTATOR analysis (Fleiss' Kappa + Krippendorff's Alpha)\n")
        calculate_multi_annotator_agreement(csv_file_path, annotator_cols)
    else:
        print("Error: Need at least 2 annotators for agreement analysis")


if __name__ == "__main__":
    # LLM to analyze
    llm = "gemini-2.5-pro"
    
    # Path to data
    auto_calculate_agreement(f"../human_evaluated_output/{llm}", annotator_cols=['Voter 1', 'Voter 2', 'Voter 3'])
    