# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Blob Storage Cost Calculator
# MAGIC
# MAGIC ### Calculate Savings: Hot Storage vs Archive Storage
# MAGIC
# MAGIC This notebook helps you estimate cost savings when moving data from **Hot Storage** to **Archive Storage** in Azure Blob Storage.
# MAGIC
# MAGIC #### Key Considerations:
# MAGIC - **Hot Storage**: Optimized for frequent access, higher storage cost, lower access cost
# MAGIC - **Archive Storage**: Optimized for rarely accessed data, lower storage cost, higher rehydration cost
# MAGIC - **Rehydration**: Process of moving data from archive back to hot tier for access

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import Required Libraries

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import json
from datetime import datetime

# Set seaborn style for professional-looking charts
sns.set_style("whitegrid")
sns.set_context("notebook", font_scale=1.1)

# Use a professional color palette suitable for executive presentations
COLORS = {
    'current': '#E74C3C',      # Red for current/warning
    'optimized': '#3498DB',    # Blue for optimized
    'savings': '#2ECC71',      # Green for savings/positive
    'hot': '#E67E22',          # Orange for hot storage
    'archive': '#1ABC9C',      # Teal for archive
    'rehydration': '#F39C12',  # Gold for rehydration
    'accent': '#9B59B6'        # Purple for accents
}

# Configure matplotlib for high-quality output
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']

print("✓ Libraries loaded successfully!")
print("✓ Professional visualization theme applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Azure Storage Pricing from Configuration File

# COMMAND ----------

# Load pricing configuration from JSON file
# Try multiple possible paths
possible_paths = [
    "./config/azure_blob_storage_pricing.json",
    # "../config/azure_blob_storage_pricing.json",
    # "/Workspace/config/azure_blob_storage_pricing.json",
    # "config/azure_blob_storage_pricing.json"
]

pricing_config = None
for config_path in possible_paths:
    try:
        with open(config_path, 'r') as f:
            pricing_config = json.load(f)
        print("Pricing configuration loaded from:", config_path)
        break
    except FileNotFoundError:
        continue

if pricing_config is None:
    print("Config file not found in any expected location, using default pricing values")

# Extract pricing values (with defaults if not in config)
# These are typical Azure East US pricing as of 2024
PRICING = {
    'hot_storage_per_gb_month': 0.0184,      # Hot tier storage cost
    'cool_storage_per_gb_month': 0.01,       # Cool tier storage cost (reference)
    'archive_storage_per_gb_month': 0.00099, # Archive tier storage cost
    
    # Read/Write operations (per 10,000 operations)
    'hot_write_operations': 0.065,           # Write operations to hot
    'hot_read_operations': 0.0044,           # Read operations from hot
    'archive_write_operations': 0.11,        # Write to archive
    
    # Rehydration costs (per GB)
    'rehydration_high_priority': 0.022,      # High priority rehydration (< 1 hour)
    'rehydration_standard': 0.011,           # Standard rehydration (< 15 hours)
    
    # Data retrieval costs (per GB)
    'hot_data_retrieval': 0.0,               # Free for hot tier
    'archive_data_retrieval': 0.022,         # Archive data retrieval (after rehydration)
}

# Display loaded pricing
print("\n Azure Blob Storage Pricing (per GB per month):")
print(f"  Hot Storage:        ${PRICING['hot_storage_per_gb_month']:.4f}")
print(f"  Archive Storage:    ${PRICING['archive_storage_per_gb_month']:.5f}")
print(f"  Rehydration (Std):  ${PRICING['rehydration_standard']:.3f}/GB")
print(f"  Rehydration (High): ${PRICING['rehydration_high_priority']:.3f}/GB")

if pricing_config:
    print(f"\n Pricing Source: {pricing_config.get('pricing_url', 'N/A')}")
    print(f"Note: {pricing_config.get('pricing_note', 'Prices may vary')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. User Input Parameters
# MAGIC
# MAGIC Enter your storage details below:
# MAGIC
# MAGIC **Note**: Rehydration % is calculated as percentage of **archived data**, not total data.
# MAGIC
# MAGIC For example: If you archive 80 TB and set rehydration to 5%, you'll rehydrate 4 TB per year.

# COMMAND ----------

# Create widgets for user input (numbered for ordering)
dbutils.widgets.text("01_current_storage_tb", "311", "01. Current Hot Storage (TB)")
dbutils.widgets.text("02_archive_candidate_tb", "40", "02. Storage to Move to Archive (TB)")
dbutils.widgets.text("03_rehydration_percent", "5", "03. Annual Rehydration (% of ARCHIVED data)")
dbutils.widgets.dropdown("04_rehydration_priority", "Standard", ["Standard", "High Priority"], "04. Rehydration Priority")
dbutils.widgets.text("05_data_growth_rate", "10", "05. Annual Data Growth Rate (%)")

# COMMAND ----------

# Get widget values
current_storage_tb = float(dbutils.widgets.get("01_current_storage_tb"))
archive_candidate_tb = float(dbutils.widgets.get("02_archive_candidate_tb"))
rehydration_percent = float(dbutils.widgets.get("03_rehydration_percent"))
rehydration_priority = dbutils.widgets.get("04_rehydration_priority")
data_growth_rate = float(dbutils.widgets.get("05_data_growth_rate")) / 100

# Convert TB to GB
current_storage_gb = current_storage_tb * 1024
archive_candidate_gb = archive_candidate_tb * 1024
remaining_hot_storage_gb = current_storage_gb - archive_candidate_gb

# Calculate actual rehydration volume for display
annual_rehydration_tb = (archive_candidate_gb / 1024) * (rehydration_percent / 100)

print(f"Parameters loaded successfully!")
print(f"  - You will rehydrate {annual_rehydration_tb:.2f} TB per year ({rehydration_percent}% of {archive_candidate_tb:.2f} TB archived)")
print(f"  - This represents {(annual_rehydration_tb/current_storage_tb)*100:.1f}% of your total current storage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cost Calculation Functions

# COMMAND ----------

def calculate_hot_storage_cost(storage_gb, months=12):
    """Calculate cost for hot storage"""
    monthly_cost = storage_gb * PRICING['hot_storage_per_gb_month']
    return monthly_cost * months

def calculate_archive_storage_cost(storage_gb, months=12):
    """Calculate cost for archive storage"""
    monthly_cost = storage_gb * PRICING['archive_storage_per_gb_month']
    return monthly_cost * months

def calculate_rehydration_cost(archive_storage_gb, rehydration_percent, priority='Standard'):
    """
    Calculate annual rehydration cost
    
    NOTE: rehydration_percent is % of ARCHIVED data, not total data
    Example: 80 TB archived with 5% rehydration = 4 TB rehydrated per year
    """
    rehydration_gb = archive_storage_gb * (rehydration_percent / 100)
    
    if priority == 'High Priority':
        rehydration_rate = PRICING['rehydration_high_priority']
    else:
        rehydration_rate = PRICING['rehydration_standard']
    
    # Rehydration cost + data retrieval cost
    cost = rehydration_gb * (rehydration_rate + PRICING['archive_data_retrieval'])
    return cost

def calculate_archive_write_cost(storage_gb):
    """One-time cost to move data to archive"""
    # Assuming 1 write operation per 4MB chunk
    operations = (storage_gb * 1024) / 4  # Convert GB to MB, then divide by 4MB chunks
    cost = (operations / 10000) * PRICING['archive_write_operations']
    return cost

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculate Current vs Optimized Costs

# COMMAND ----------

def calculate_scenario_costs(years, current_storage_gb, archive_candidate_gb, 
                            rehydration_percent, priority, growth_rate):
    """
    Calculate costs for current (all hot) vs optimized (hot + archive) scenarios
    """
    results = {
        'year': [],
        'current_hot_storage_gb': [],
        'optimized_hot_storage_gb': [],
        'optimized_archive_storage_gb': [],
        'current_scenario_cost': [],
        'optimized_scenario_cost': [],
        'annual_savings': [],
        'cumulative_savings': []
    }
    
    cumulative_savings = 0
    archive_write_cost = calculate_archive_write_cost(archive_candidate_gb)
    
    for year in range(1, years + 1):
        # Apply growth rate
        current_year_storage = current_storage_gb * ((1 + growth_rate) ** (year - 1))
        remaining_hot = (current_storage_gb - archive_candidate_gb) * ((1 + growth_rate) ** (year - 1))
        archive_storage = archive_candidate_gb * ((1 + growth_rate) ** (year - 1))
        
        # Current scenario: All data in hot storage
        current_cost = calculate_hot_storage_cost(current_year_storage, months=12)
        
        # Optimized scenario: Some in hot, some in archive
        hot_cost = calculate_hot_storage_cost(remaining_hot, months=12)
        archive_cost = calculate_archive_storage_cost(archive_storage, months=12)
        rehydration_cost = calculate_rehydration_cost(archive_storage, rehydration_percent, priority)
        
        # Add one-time archive write cost in year 1
        optimized_cost = hot_cost + archive_cost + rehydration_cost
        if year == 1:
            optimized_cost += archive_write_cost
        
        # Calculate savings
        annual_savings = current_cost - optimized_cost
        cumulative_savings += annual_savings
        
        # Store results
        results['year'].append(year)
        results['current_hot_storage_gb'].append(current_year_storage)
        results['optimized_hot_storage_gb'].append(remaining_hot)
        results['optimized_archive_storage_gb'].append(archive_storage)
        results['current_scenario_cost'].append(current_cost)
        results['optimized_scenario_cost'].append(optimized_cost)
        results['annual_savings'].append(annual_savings)
        results['cumulative_savings'].append(cumulative_savings)
    
    return pd.DataFrame(results)

# COMMAND ----------

# Calculate for 10 years
results_df = calculate_scenario_costs(
    years=10,
    current_storage_gb=current_storage_gb,
    archive_candidate_gb=archive_candidate_gb,
    rehydration_percent=rehydration_percent,
    priority=rehydration_priority,
    growth_rate=data_growth_rate
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Results Summary

# COMMAND ----------

# Display summary for 1, 5, and 10 years
summary_years = [1, 5, 10]
summary_data = []

for year in summary_years:
    year_data = results_df[results_df['year'] == year].iloc[0]
    summary_data.append({
        'Time Period': f'Year {year}',
        'Current Scenario Cost ($)': f"${year_data['current_scenario_cost']:,.2f}",
        'Optimized Scenario Cost ($)': f"${year_data['optimized_scenario_cost']:,.2f}",
        'Annual Savings ($)': f"${year_data['annual_savings']:,.2f}",
        'Cumulative Savings ($)': f"${year_data['cumulative_savings']:,.2f}"
    })

summary_df = pd.DataFrame(summary_data)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Detailed Breakdown

# COMMAND ----------

print("=" * 80)
print(f"AZURE BLOB STORAGE COST ANALYSIS")
print("=" * 80)
print(f"\nInput Parameters:")
print(f"  Current Hot Storage:           {current_storage_tb:,.2f} TB ({current_storage_gb:,.2f} GB)")
print(f"  Storage Moving to Archive:     {archive_candidate_tb:,.2f} TB ({archive_candidate_gb:,.2f} GB)")
print(f"  Remaining in Hot Storage:      {(current_storage_gb - archive_candidate_gb)/1024:,.2f} TB")
print(f"  Annual Rehydration:            {rehydration_percent}% of archived data")
print(f"  Rehydration Priority:          {rehydration_priority}")
print(f"  Annual Data Growth Rate:       {data_growth_rate * 100}%")
print("\n" + "=" * 80)

# Year 1 breakdown
year1 = results_df[results_df['year'] == 1].iloc[0]
print(f"\n YEAR 1 DETAILED BREAKDOWN")
print("=" * 80)
print(f"\nCurrent Scenario (All Hot Storage):")
print(f"  Storage Cost:                  ${year1['current_scenario_cost']:,.2f}")

print(f"\nOptimized Scenario (Hot + Archive):")
hot_cost_y1 = calculate_hot_storage_cost(year1['optimized_hot_storage_gb'], 12)
archive_cost_y1 = calculate_archive_storage_cost(year1['optimized_archive_storage_gb'], 12)
rehydration_cost_y1 = calculate_rehydration_cost(year1['optimized_archive_storage_gb'], 
                                                   rehydration_percent, rehydration_priority)
write_cost = calculate_archive_write_cost(archive_candidate_gb)

print(f"  Hot Storage Cost:              ${hot_cost_y1:,.2f}")
print(f"  Archive Storage Cost:          ${archive_cost_y1:,.2f}")
print(f"  Rehydration Cost (Annual):     ${rehydration_cost_y1:,.2f}")
print(f"  One-time Archive Write Cost:   ${write_cost:,.2f}")
print(f"  Total Optimized Cost:          ${year1['optimized_scenario_cost']:,.2f}")

print(f"\n SAVINGS:")
print(f"  Year 1 Savings:                ${year1['annual_savings']:,.2f}")
print(f"  ROI:                           {(year1['annual_savings']/year1['current_scenario_cost']*100):.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Multi-Year Projections

# COMMAND ----------

# Display detailed 10-year projection
projection_df = results_df.copy()
projection_df['current_hot_storage_tb'] = projection_df['current_hot_storage_gb'] / 1024
projection_df['optimized_hot_storage_tb'] = projection_df['optimized_hot_storage_gb'] / 1024
projection_df['optimized_archive_storage_tb'] = projection_df['optimized_archive_storage_gb'] / 1024

display_df = projection_df[[
    'year', 
    'current_hot_storage_tb',
    'optimized_hot_storage_tb',
    'optimized_archive_storage_tb',
    'current_scenario_cost', 
    'optimized_scenario_cost',
    'annual_savings',
    'cumulative_savings'
]].round(2)

display_df.columns = [
    'Year',
    'Current Hot (TB)',
    'Optimized Hot (TB)',
    'Archive (TB)',
    'Current Cost ($)',
    'Optimized Cost ($)',
    'Annual Savings ($)',
    'Cumulative Savings ($)'
]

display(display_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cost Analysis - Simple & Clear Visualizations

# COMMAND ----------

# Create simple, executive-ready visualizations
fig, axes = plt.subplots(2, 2, figsize=(18, 11))
fig.suptitle('Azure Storage Archival - Cost Savings Analysis', 
             fontsize=20, fontweight='bold', y=0.995)

# 1. MAIN CHART: Cost Comparison - Current vs Optimized
ax1 = axes[0, 0]
sns.lineplot(x='year', y='current_scenario_cost', data=results_df, 
             marker='o', markersize=12, linewidth=4, label='Current: All Hot Storage', 
             color=COLORS['current'], ax=ax1)
sns.lineplot(x='year', y='optimized_scenario_cost', data=results_df, 
             marker='s', markersize=12, linewidth=4, label='Optimized: Hot + Archive', 
             color=COLORS['optimized'], ax=ax1)
ax1.fill_between(results_df['year'], results_df['current_scenario_cost'], 
                 results_df['optimized_scenario_cost'], alpha=0.2, color=COLORS['savings'])
ax1.set_xlabel('Year', fontsize=13, fontweight='bold')
ax1.set_ylabel('Annual Cost ($)', fontsize=13, fontweight='bold')
ax1.set_title('What We Pay Now vs. What We Could Pay', fontsize=15, fontweight='bold', pad=15)
ax1.legend(fontsize=12, frameon=True, shadow=True, loc='upper left')
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
ax1.grid(True, alpha=0.3)

# 2. CUMULATIVE SAVINGS: How Much We Save Over Time
ax2 = axes[0, 1]
ax2.fill_between(results_df['year'], results_df['cumulative_savings'], 
                  alpha=0.4, color=COLORS['savings'])
sns.lineplot(x='year', y='cumulative_savings', data=results_df, 
             marker='D', markersize=12, linewidth=4, color=COLORS['savings'], ax=ax2)
ax2.set_xlabel('Year', fontsize=13, fontweight='bold')
ax2.set_ylabel('Total Savings ($)', fontsize=13, fontweight='bold')
ax2.set_title('How Much Money We Save', fontsize=15, fontweight='bold', pad=15)
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
ax2.grid(True, alpha=0.3)

# Add milestone labels
for year in [1, 5, 10]:
    value = results_df[results_df['year'] == year]['cumulative_savings'].values[0]
    ax2.text(year, value, f'${value/1000:,.0f}K', ha='center', va='bottom', 
             fontsize=11, fontweight='bold')

# 3. ANNUAL SAVINGS: Year-by-Year Savings
ax3 = axes[1, 0]
bars = ax3.bar(results_df['year'], results_df['annual_savings'], 
               color=COLORS['savings'], alpha=0.85, edgecolor='black', linewidth=2)
ax3.set_xlabel('Year', fontsize=13, fontweight='bold')
ax3.set_ylabel('Annual Savings ($)', fontsize=13, fontweight='bold')
ax3.set_title('Savings Per Year', fontsize=15, fontweight='bold', pad=15)
ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
ax3.axhline(y=0, color='black', linestyle='-', linewidth=2)
ax3.grid(True, alpha=0.3, axis='y')

# 4. KEY METRICS SUMMARY
ax4 = axes[1, 1]
ax4.axis('off')

# Calculate key metrics
total_10yr_savings = results_df['cumulative_savings'].iloc[-1]
year1_savings = results_df['annual_savings'].iloc[0]
year1_roi = (year1_savings / results_df['current_scenario_cost'].iloc[0] * 100)
avg_annual_savings = results_df['annual_savings'].mean()

summary_text = f"""
KEY FINDINGS

> 10-Year Total Savings
   ${total_10yr_savings:,.0f}

> First Year Savings
   ${year1_savings:,.0f}
   ROI: {year1_roi:.1f}%

> Average Annual Savings
   ${avg_annual_savings:,.0f}/year

>  Storage Strategy
   Move {archive_candidate_tb:.0f} TB to Archive
   ({(archive_candidate_gb/current_storage_gb*100):.0f}% of total data)
   
   Rehydrate {rehydration_percent}% annually
   ({annual_rehydration_tb:.1f} TB/year)

> RECOMMENDATION:
   {"IMPLEMENT - Strong cost savings" if year1_savings > 0 else "REVIEW - Check assumptions"}
"""

ax4.text(0.1, 0.95, summary_text, transform=ax4.transAxes,
         fontsize=13, verticalalignment='top', family='monospace', fontweight='bold',
         bbox=dict(boxstyle='round', facecolor=COLORS['savings'], alpha=0.15, 
                  edgecolor=COLORS['savings'], linewidth=3, pad=1.5))

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Key Insights & Recommendations

# COMMAND ----------

# Calculate key metrics
total_10yr_savings = results_df['cumulative_savings'].iloc[-1]
avg_annual_savings = results_df['annual_savings'].mean()
payback_period = 0
for idx, row in results_df.iterrows():
    if row['cumulative_savings'] > 0:
        payback_period = row['year']
        break

print("=" * 80)
print("KEY INSIGHTS & RECOMMENDATIONS")
print("=" * 80)

print(f"\nFinancial Impact:")
print(f"  • 10-Year Total Savings:       ${total_10yr_savings:,.2f}")
print(f"  • Average Annual Savings:      ${avg_annual_savings:,.2f}")
print(f"  • Year 1 ROI:                  {(results_df['annual_savings'].iloc[0]/results_df['current_scenario_cost'].iloc[0]*100):.1f}%")
if payback_period > 0:
    print(f"  • Payback Period:              {payback_period} year(s)")

print(f"\nStorage Optimization:")
archive_percentage = (archive_candidate_gb / current_storage_gb) * 100
annual_rehydration_volume = archive_candidate_gb * rehydration_percent/100/1024
print(f"  • Data Moving to Archive:      {archive_percentage:.1f}% of total storage")
print(f"  • Archive Storage Cost:        {(PRICING['archive_storage_per_gb_month']/PRICING['hot_storage_per_gb_month']*100):.1f}% of hot storage cost")
print(f"  • Annual Rehydration:          {rehydration_percent}% of ARCHIVED data = {annual_rehydration_volume:.2f} TB/year")
print(f"    (This is {(annual_rehydration_volume/(current_storage_tb))*100:.1f}% of total current storage)")

print(f"\nImportant Considerations:")
print(f"  • Rehydration time for standard priority: Up to 15 hours")
print(f"  • Rehydration time for high priority: Less than 1 hour")
print(f"  • Archive storage is ideal for data accessed < 2 times per year")
print(f"  • Consider tiering strategy: Hot → Cool → Archive based on access patterns")

print(f"\nRecommendations:")
if archive_percentage > 80:
    print(f"  You're moving {archive_percentage:.0f}% to archive - ensure this data is rarely accessed")
elif archive_percentage < 20:
    print(f"  Consider moving more data to archive for additional savings")
else:
    print(f"  Good balance between hot and archive storage")

if rehydration_percent > 10:
    print(f"  High rehydration rate ({rehydration_percent}%) may reduce savings")
    print(f"     Consider keeping frequently accessed data in hot/cool tier")
else:
    print(f"  Low rehydration rate is optimal for archive storage")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Export Results

# COMMAND ----------

# Save results to Delta table (optional)
# Uncomment if you want to save results

# results_df['calculation_date'] = datetime.now()
# results_df['scenario_name'] = f"Archive_{archive_candidate_tb}TB_{rehydration_percent}pct_rehydration"
# 
# results_df.write.format("delta").mode("append").saveAsTable("storage_cost_analysis")
# print("Results saved to 'storage_cost_analysis' table")

# Export to CSV for sharing
# output_path = "/dbfs/FileStore/storage_cost_analysis.csv"
# display_df.to_csv(output_path, index=False)
# print(f"Results exported to: {output_path}")
# print(f"\nSummary:")
# print(f"  - Archived Data: {archive_candidate_tb:.2f} TB")
# print(f"  - Annual Rehydration: {rehydration_percent}% of archived data = {annual_rehydration_volume:.2f} TB")
# print(f"  - 10-Year Total Savings: ${results_df['cumulative_savings'].iloc[-1]:,.2f}")
