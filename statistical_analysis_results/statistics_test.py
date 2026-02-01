import pandas as pd
import numpy as np
from scipy.stats import shapiro, f_oneway, kruskal, mannwhitneyu, ttest_ind
from scipy.stats import tukey_hsd
from itertools import combinations
import matplotlib.pyplot as plt
import seaborn as sns

# Substitute with the actual path to the CSV for wich you want to perform the statistical analysis
df = pd.read_csv("RV_summary_Co.csv")

df_long = df.melt(
    id_vars="LLM",
    var_name="format",
    value_name="score"
)

normality_results = {}

for fmt in df_long["format"].unique():
    values = df_long[df_long["format"] == fmt]["score"]
    stat, p = shapiro(values)
    normality_results[fmt] = p

print("Shapiro–Wilk normality p-values per format:")
for fmt, p in normality_results.items():
    print(f"{fmt}: p = {p:.4f}")

all_normal = all(p > 0.05 for p in normality_results.values())

groups = [
    df_long[df_long["format"] == fmt]["score"].values
    for fmt in df_long["format"].unique()
]

if all_normal:
    stat, p = f_oneway(*groups)
    test_name = "One-way ANOVA"
else:
    stat, p = kruskal(*groups)
    test_name = "Kruskal–Wallis"

print("\nSelected test:", test_name)
print(f"Statistic = {stat:.4f}")
print(f"p-value   = {p:.6f}")

print("\n" + "="*60)
print("DESCRIPTIVE STATISTICS BY FORMAT")
print("="*60)

summary = df_long.groupby("format")["score"].agg([
    ('mean', 'mean'),
    ('median', 'median'),
    ('std', 'std'),
    ('min', 'min'),
    ('max', 'max'),
    ('count', 'count')
]).round(4)

summary = summary.sort_values('median', ascending=False)
print(summary)

print("\n" + "="*60)
print("BEST FORMAT (by median score):")
print("="*60)
best_format = summary.index[0]
best_median = summary.loc[best_format, 'median']
print(f"🏆 {best_format}")
print(f"   Median score: {best_median:.4f}")
print(f"   Mean score:   {summary.loc[best_format, 'mean']:.4f}")


if p < 0.05:
    print("\n" + "="*60)
    
    if all_normal:
        # ANOVA - Use Tukey's HSD
        print("POST-HOC TEST: Tukey's HSD (for ANOVA)")
        print("="*60)
        
        # Tukey's HSD
        res = tukey_hsd(*groups)
        
        formats = df_long["format"].unique()
        best_idx = list(formats).index(best_format)
        
        print(f"\nComparing best format ({best_format}) against others:\n")
        pairwise_results = []
        
        for i, fmt in enumerate(formats):
            if fmt != best_format:
                p_val = res.pvalue[best_idx, i]
                pairwise_results.append({
                    'Comparison': f'{best_format} vs {fmt}',
                    'p-value': p_val,
                    'Significant': '✓' if p_val < 0.05 else '✗'
                })
        
        pairwise_df = pd.DataFrame(pairwise_results)
        print(pairwise_df.to_string(index=False))
        
    else:
        # Kruskal-Wallis - Use Mann-Whitney U
        print("POST-HOC TEST: Mann-Whitney U (for Kruskal-Wallis)")
        print("="*60)
        print(f"Comparing best format ({best_format}) against others:\n")
        
        formats = df_long["format"].unique()
        best_scores = df_long[df_long["format"] == best_format]["score"].values
        
        pairwise_results = []
        
        for fmt in formats:
            if fmt != best_format:
                fmt_scores = df_long[df_long["format"] == fmt]["score"].values
                u_stat, p_val = mannwhitneyu(best_scores, fmt_scores, alternative='two-sided')
                
                pairwise_results.append({
                    'Comparison': f'{best_format} vs {fmt}',
                    'U-statistic': u_stat,
                    'p-value': p_val,
                    'Significant': '✓' if p_val < 0.05 else '✗'
                })
        
        pairwise_df = pd.DataFrame(pairwise_results)
        print(pairwise_df.to_string(index=False))
else:
    print("\n Overall test not significant (p >= 0.05)")
    print("   No strong evidence of differences between formats")

# Visualization
print("\n" + "="*60)
print("GENERATING VISUALIZATION...")
print("="*60)

plt.figure(figsize=(12, 6))
sns.boxplot(data=df_long, x='format', y='score', order=summary.index)
plt.xticks(rotation=45, ha='right')
plt.title(f'{test_name} Test: Format Comparison (p={p:.4f})')
plt.ylabel('Score')
plt.xlabel('Format')
plt.axhline(y=best_median, color='red', linestyle='--', alpha=0.5, label=f'Best median: {best_median:.4f}')
plt.legend()
plt.tight_layout()
plt.savefig('format_comparison.png', dpi=300, bbox_inches='tight')
print("✓ Saved visualization as 'format_comparison.png'")
plt.show()