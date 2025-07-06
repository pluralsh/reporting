import pandas as pd
import re
from datetime import datetime
from collections import defaultdict
import os
import sys
import zipfile
import io

def count_unique_developers(users_df):
    """Count unique developers based on email addresses"""
    return len(users_df['email'].str.lower().unique())

def get_environment(service_name):
    """Extract environment from service name"""
    parts = service_name.split('-')
    for part in parts:
        if part.lower() in ['dev', 'qa', 'prod', 'prd', 'sbx', 'staging', 'test']:
            return part.lower()
    return None

def get_github_org(repo_url):
    """Extract GitHub organization from repository URL"""
    if pd.isna(repo_url):
        return None
        
    if repo_url.startswith('git@'):
        parts = repo_url.split(':')
        if len(parts) > 1:
            org = parts[1].split('/')[0]
            return org
            
    elif repo_url.startswith('http'):
        parts = repo_url.split('/')
        if len(parts) > 3:
            return parts[3]
            
    return None

def analyze_monthly_creations(services_df, workspaces):
    """Analyze workload creation by month"""
    # Filter services to only include workloads (services that belong to a workspace)
    workload_mask = services_df['service'].apply(
        lambda s: any(str(s).startswith(w + '-') for w in workspaces.keys()) 
        and not str(s).endswith('-runtime') if pd.notna(s) else False
    )
    workload_df = services_df[workload_mask].copy()
    
    workload_df['created_at'] = pd.to_datetime(workload_df['created_at'])
    
    monthly_counts = workload_df.groupby(workload_df['created_at'].dt.strftime('%B'))['service'].count()
    
    all_months = ['January', 'February', 'March', 'April', 'May', 'June', 
                 'July', 'August', 'September', 'October', 'November', 'December']
    monthly_counts = monthly_counts.reindex(all_months, fill_value=0)
    
    return monthly_counts.to_dict()

def analyze_services(services_df):
    """Analyze services to count unique workspaces and workloads"""
    # Initialize workspace tracking
    workspaces = {}  # Will store workspace_name -> {prefix, workloads, env_workloads, org_workloads}
    
    # First pass: identify workspaces by finding -runtime services
    for service in services_df['service']:
        if pd.isna(service):
            continue
        
        if service.endswith('-runtime'):
            # Remove -runtime to get the workspace prefix
            workspace_prefix = service[:-8]  # len('-runtime') = 8
            workspaces[workspace_prefix] = {
                'prefix': workspace_prefix,
                'workloads': set(),  # Will store all workload names for this workspace
                'env_workloads': defaultdict(set),  # Will store workloads by environment
                'org_workloads': defaultdict(set)  # Will store workloads by GitHub org
            }
    
    # Second pass: count workloads, environments, and orgs for each workspace
    for _, row in services_df.iterrows():
        service = row['service']
        if pd.isna(service):
            continue
            
        # Find which workspace this service belongs to
        for workspace_prefix in workspaces:
            if service.startswith(workspace_prefix + '-'):
                if not service.endswith('-runtime'):  # Don't count runtime services as workloads
                    workspaces[workspace_prefix]['workloads'].add(service)
                    
                    # Extract and add environment
                    env = get_environment(service)
                    if env:
                        workspaces[workspace_prefix]['env_workloads'][env].add(service)
                    
                    # Extract and add GitHub org
                    org = get_github_org(row['repository'])
                    if org:
                        workspaces[workspace_prefix]['org_workloads'][org].add(service)
                break
    
    # Create workspace statistics
    workspace_stats = {}
    total_workloads = 0
    for workspace_prefix, data in workspaces.items():
        # Calculate environment distribution
        env_distribution = {
            env: len(workloads) 
            for env, workloads in data['env_workloads'].items()
        }
        
        # Calculate org distribution
        org_distribution = {
            org: len(workloads)
            for org, workloads in data['org_workloads'].items()
        }
        
        workspace_stats[workspace_prefix] = {
            'total_workloads': len(data['workloads']),
            'workload_names': sorted(list(data['workloads'])),
            'env_distribution': env_distribution,
            'environments': sorted(env_distribution.keys()),
            'org_distribution': org_distribution,
            'organizations': sorted(org_distribution.keys())
        }
        total_workloads += len(data['workloads'])

    return len(workspaces), total_workloads, workspace_stats, workspaces

def export_results_to_zip(metrics, workspace_stats, monthly_counts):
    """Export results to a zip file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"report_{timestamp}.zip"
    
    summary_df = pd.DataFrame([metrics])
    
    workspace_details = []
    for workspace, stats in workspace_stats.items():
        env_data = {
            f'workloads_in_{env}': stats['env_distribution'].get(env, 0)
            for env in ['dev', 'qa', 'prod', 'prd', 'sbx', 'staging', 'test']
        }
        
        all_orgs = set()
        for ws_stats in workspace_stats.values():
            all_orgs.update(ws_stats['org_distribution'].keys())
        
        org_data = {
            f'workloads_in_{org}': stats['org_distribution'].get(org, 0)
            for org in sorted(all_orgs)
        }
        
        workspace_details.append({
            'workspace_name': workspace,
            'total_workload_count': stats['total_workloads'],
            **env_data,  # Add environment distribution columns
            **org_data   # Add organization distribution columns
        })
    
    workspace_df = pd.DataFrame(workspace_details).sort_values('total_workload_count', ascending=False)
    
    monthly_df = pd.DataFrame([monthly_counts]).T
    monthly_df.columns = ['workload_count']
    monthly_df.index.name = 'month'
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Write summary
        summary_buffer = io.StringIO()
        summary_df.to_csv(summary_buffer, index=False)
        zipf.writestr(f'metrics_summary_{timestamp}.csv', summary_buffer.getvalue())
        
        # Write workspace details
        workspace_buffer = io.StringIO()
        workspace_df.to_csv(workspace_buffer, index=False)
        zipf.writestr(f'workspace_details_{timestamp}.csv', workspace_buffer.getvalue())
        
        # Write monthly counts
        monthly_buffer = io.StringIO()
        monthly_df.to_csv(monthly_buffer)
        zipf.writestr(f'workloads_created_monthly_{timestamp}.csv', monthly_buffer.getvalue())
    
    return zip_filename

def main():
    try:
        services_csv = os.environ.get('SERVICES_CSV_PATH', '')
        users_csv = os.environ.get('USERS_CSV_PATH', '')
            
        if not services_csv:
            print("Error: SERVICES_CSV_PATH environment variable is not set")
            sys.exit(1)
        
        users_df = pd.read_csv(users_csv)
        services_df = pd.read_csv(services_csv)
        
        # Calculate metrics
        unique_developers = count_unique_developers(users_df)
        unique_workspaces, total_workloads, workspace_stats, workspaces = analyze_services(services_df)
        monthly_counts = analyze_monthly_creations(services_df, workspaces)
        
        metrics = {
            'number_of_unique_developers': unique_developers,
            'number_of_unique_workspaces': unique_workspaces,
            'total_number_of_workloads': total_workloads
        }
        
        zip_file = export_results_to_zip(metrics, workspace_stats, monthly_counts)
        
        print("\nAnalysis Results:")
        print("-----------------")
        print(f"Number of unique developers: {unique_developers}")
        print(f"Number of unique workspaces: {unique_workspaces}")
        print(f"Total number of workloads: {total_workloads}")
        
        print(f"\nMonthly Workload Creation:")
        print("--------------------------")
        total_monthly = 0
        for month in ['January', 'February', 'March', 'April', 'May', 'June', 
                     'July', 'August', 'September', 'October', 'November', 'December']:
            count = monthly_counts[month]
            total_monthly += count
            print(f"  {month}: {count} workloads")
        print(f"\nTotal workloads created: {total_monthly}")
        
        print(f"\nTop 5 workspaces by workload count:")
        for workspace, stats in sorted(workspace_stats.items(), 
                                     key=lambda x: x[1]['total_workloads'], 
                                     reverse=True)[:5]:
            print(f"  {workspace}:")
            print(f"    - {stats['total_workloads']} total workloads")
            print(f"    - Distribution across environments:")
            for env, count in sorted(stats['env_distribution'].items()):
                print(f"      * {env}: {count} workloads")
            print(f"    - Distribution across organizations:")
            for org, count in sorted(stats['org_distribution'].items()):
                print(f"      * {org}: {count} workloads")
            if stats['total_workloads'] > 0:
                print(f"    - Example workloads:")
                for workload in sorted(stats['workload_names'])[:3]:  # Show up to 3 examples
                    print(f"      * {workload}")
        
        print(f"\nResults have been exported to: {zip_file}")
        
    except FileNotFoundError as e:
        print(f"Error: Could not find one of the required CSV files: {e}")
    except Exception as e:
        print(f"Error occurred while processing the files: {e}")

if __name__ == "__main__":
    main() 