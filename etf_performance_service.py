from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

app = FastAPI(title="ETF Performance Service", version="1.0.0")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')

def get_latest_etf_performance_file():
    """Find the latest recent-etf-performance-{ddmmyyyy}.csv file"""
    pattern = os.path.join(OUTPUT_DIR, "recent-etf-performance-*.csv")
    files = glob.glob(pattern)
    
    if not files:
        return None
    
    # Sort by modification time to get the latest
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def get_latest_india_etf_performance_file():
    """Find the latest india_etf_performance_{ddmmyyyy}.csv file"""
    pattern = os.path.join(OUTPUT_DIR, "india_etf_performance_*.csv")
    files = glob.glob(pattern)

    if not files:
        return None

    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def load_etf_data():
    """Load the latest ETF performance data"""
    latest_file = get_latest_etf_performance_file()
    
    if not latest_file:
        raise HTTPException(status_code=404, detail="No ETF performance data found")
    
    try:
        df = pd.read_csv(latest_file)
        file_date = os.path.basename(latest_file).replace("recent-etf-performance-", "").replace(".csv", "")
        return df, file_date
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")


def load_india_etf_data():
    """Load the latest India ETF performance data"""
    latest_file = get_latest_india_etf_performance_file()

    if not latest_file:
        raise HTTPException(status_code=404, detail="No India ETF performance data found")

    try:
        df = pd.read_csv(latest_file)
        file_date = os.path.basename(latest_file).replace("india_etf_performance_", "").replace(".csv", "")
        return df, file_date
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "message": "ETF Performance Service API",
        "endpoints": {
            "/api/etf-performance": "Get all ETF performance data in JSON format",
            "/api/etf-performance/group/{group}": "Get ETF performance data for a specific group",
            "/dashboard": "Interactive HTML dashboard with graphs",
            "/health": "Health check endpoint"
        }
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    latest_file = get_latest_etf_performance_file()
    return {
        "status": "healthy",
        "latest_file": os.path.basename(latest_file) if latest_file else None,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/etf-performance")
def get_etf_performance():
    """Get all ETF performance data in JSON format"""
    df, file_date = load_etf_data()
    
    # Convert DataFrame to JSON-friendly format
    data = df.to_dict(orient='records')
    
    return {
        "data_date": file_date,
        "total_etfs": len(data),
        "groups": df['Group'].unique().tolist(),
        "etfs": data
    }

@app.get("/api/etf-performance/group/{group}")
def get_etf_performance_by_group(group: str):
    """Get ETF performance data for a specific group"""
    df, file_date = load_etf_data()
    
    # Filter by group (case-insensitive)
    group_data = df[df['Group'].str.lower() == group.lower()]
    
    if group_data.empty:
        raise HTTPException(
            status_code=404, 
            detail=f"Group '{group}' not found. Available groups: {df['Group'].unique().tolist()}"
        )
    
    data = group_data.to_dict(orient='records')
    
    return {
        "data_date": file_date,
        "group": group,
        "total_etfs": len(data),
        "etfs": data
    }

@app.get("/api/etf-performance/top/{n}")
def get_top_performers(n: int = 10):
    """Get top N performers by 1-week performance"""
    df, file_date = load_etf_data()
    
    # Sort by 1-week performance and get top N
    top_etfs = df.nlargest(n, '1_Week_Performance_%')
    data = top_etfs.to_dict(orient='records')
    
    return {
        "data_date": file_date,
        "top_n": n,
        "etfs": data
    }

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """Interactive HTML dashboard with graphs by group"""
    try:
        df, file_date = load_etf_data()
    except HTTPException as e:
        return f"<html><body><h1>Error: {e.detail}</h1></body></html>"
    
    # Parse date for display
    try:
        date_obj = datetime.strptime(file_date, "%d%m%Y")
        display_date = date_obj.strftime("%B %d, %Y")
    except:
        display_date = file_date
    
    # Get unique groups
    groups = df['Group'].unique()
    
    # Create individual graphs for each group
    graphs_html = []
    
    for group in groups:
        group_data = df[df['Group'] == group].copy()
        
        # Sort by 1-week performance
        group_data = group_data.sort_values('1_Week_Performance_%', ascending=True)
        
        # Take top 15 and bottom 10 if more than 25 ETFs
        if len(group_data) > 25:
            top_15 = group_data.tail(15)
            bottom_10 = group_data.head(10)
            group_data = pd.concat([bottom_10, top_15])
        
        # Prepare customdata with ETF_Name and AUM
        # Handle missing AUM values
        aum_display = group_data['AUM'].fillna('N/A')
        customdata = np.column_stack((group_data['ETF_Name'], aum_display))
        
        # Create horizontal bar chart for performance comparison
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                f'{group} - 1 Week Performance',
                f'{group} - 1 Month Performance',
                f'{group} - 6 Month Performance',
                f'{group} - YTD Performance'
            ),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        # 1-Week Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['1_Week_Performance_%'],
                orientation='h',
                name='1 Week',
                marker=dict(color=group_data['1_Week_Performance_%'], 
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['1_Week_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>AUM: %{customdata[1]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 1-Month Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['1_Month_Performance_%'],
                orientation='h',
                name='1 Month',
                marker=dict(color=group_data['1_Month_Performance_%'],
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['1_Month_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>AUM: %{customdata[1]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=1, col=2
        )
        
        # 6-Month Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['6_Month_Performance_%'],
                orientation='h',
                name='6 Month',
                marker=dict(color=group_data['6_Month_Performance_%'],
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['6_Month_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>AUM: %{customdata[1]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=2, col=1
        )
        
        # YTD Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['YTD_Performance_%'],
                orientation='h',
                name='YTD',
                marker=dict(color=group_data['YTD_Performance_%'],
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['YTD_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>AUM: %{customdata[1]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_xaxes(title_text="Performance (%)", row=1, col=1)
        fig.update_xaxes(title_text="Performance (%)", row=1, col=2)
        fig.update_xaxes(title_text="Performance (%)", row=2, col=1)
        fig.update_xaxes(title_text="Performance (%)", row=2, col=2)
        
        fig.update_layout(
            height=800,
            showlegend=False,
            title_text=f"{group} ETFs Performance Analysis",
            title_x=0.5,
            title_font_size=20
        )
        
        graphs_html.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))
        
        # Add trend distribution pie chart
        trend_fig = go.Figure()
        
        trend_counts = group_data['Trend_SMA'].value_counts()
        
        trend_fig.add_trace(go.Pie(
            labels=trend_counts.index,
            values=trend_counts.values,
            hole=0.3,
            marker=dict(colors=['#2ecc71', '#e74c3c', '#95a5a6', '#34495e'])
        ))
        
        trend_fig.update_layout(
            title_text=f"{group} ETFs - Trend Distribution (SMA Method)",
            title_x=0.5,
            height=400
        )
        
        graphs_html.append(trend_fig.to_html(full_html=False, include_plotlyjs='cdn'))
        
        # Add AUM comparison chart
        # Filter out ETFs with no AUM data and sort by AUM
        group_data_with_aum = group_data[group_data['AUM_Billions'].notna()].copy()
        
        if len(group_data_with_aum) > 0:
            group_data_with_aum = group_data_with_aum.sort_values('AUM_Billions', ascending=True)
            
            # Take top 20 if more than 20 ETFs
            if len(group_data_with_aum) > 20:
                group_data_with_aum = group_data_with_aum.tail(20)
            
            aum_fig = go.Figure()
            
            # Prepare customdata for hover
            aum_customdata = np.column_stack((
                group_data_with_aum['ETF_Name'],
                group_data_with_aum['AUM']
            ))
            
            aum_fig.add_trace(go.Bar(
                y=group_data_with_aum['ETF_Ticker'],
                x=group_data_with_aum['AUM_Billions'],
                orientation='h',
                marker=dict(
                    color=group_data_with_aum['AUM_Billions'],
                    colorscale='Blues',
                    showscale=True,
                    colorbar=dict(title="AUM (Billions)")
                ),
                text=group_data_with_aum['AUM'],
                textposition='outside',
                customdata=aum_customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>AUM: %{customdata[1]}<extra></extra>'
            ))
            
            aum_fig.update_layout(
                title_text=f"{group} ETFs - Assets Under Management (AUM)",
                title_x=0.5,
                xaxis_title="AUM (Billions USD)",
                height=max(400, len(group_data_with_aum) * 25),
                showlegend=False
            )
            
            graphs_html.append(aum_fig.to_html(full_html=False, include_plotlyjs='cdn'))
    
    # Summary statistics
    total_etfs = len(df)
    avg_1week = df['1_Week_Performance_%'].mean()
    avg_1month = df['1_Month_Performance_%'].mean()
    avg_ytd = df['YTD_Performance_%'].mean()
    
    # Top 5 performers
    top_5 = df.nlargest(5, '1_Week_Performance_%')[['ETF_Ticker', 'ETF_Name', 'Group', 'AUM', '1_Week_Performance_%', '1_Month_Performance_%']]
    
    # Create top performers table HTML
    top_performers_html = top_5.to_html(index=False, classes='table table-striped', border=0)
    
    # Combine all graphs
    all_graphs = '\n'.join(graphs_html)
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>ETF Performance Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f5f5;
                padding: 20px;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }}
            .stats-card {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            .stat-value {{
                font-size: 2em;
                font-weight: bold;
                color: #667eea;
            }}
            .stat-label {{
                color: #666;
                font-size: 0.9em;
            }}
            .graph-container {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }}
            .table {{
                background: white;
            }}
            h2 {{
                color: #333;
                margin-top: 30px;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container-fluid">
            <div class="header">
                <h1>🚀 ETF Performance Dashboard</h1>
                <p class="mb-0">Data as of: <strong>{display_date}</strong></p>
                <p class="mb-0">Total ETFs Analyzed: <strong>{total_etfs}</strong></p>
            </div>
            
            <div class="row">
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Average 1-Week Performance</div>
                        <div class="stat-value">{avg_1week:.2f}%</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Average 1-Month Performance</div>
                        <div class="stat-value">{avg_1month:.2f}%</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Average YTD Performance</div>
                        <div class="stat-value">{avg_ytd:.2f}%</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Groups Analyzed</div>
                        <div class="stat-value">{len(groups)}</div>
                    </div>
                </div>
            </div>
            
            <div class="graph-container">
                <h2>📊 Top 5 Performers (1-Week)</h2>
                {top_performers_html}
            </div>
            
            <h2>📈 Performance by Group</h2>
            
            {all_graphs}
            
            <div class="footer text-center mt-5 mb-3">
                <p class="text-muted">
                    <a href="/api/etf-performance" class="btn btn-primary me-2">View JSON API</a>
                    <a href="/docs" class="btn btn-secondary">API Documentation</a>
                </p>
                <p class="text-muted small">ETF Performance Service v1.0.0</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html_content


@app.get("/indiadashboard", response_class=HTMLResponse)
def india_dashboard():
    """Interactive HTML dashboard for India ETF data"""
    try:
        df, file_date = load_india_etf_data()
    except HTTPException as e:
        return f"<html><body><h1>Error: {e.detail}</h1></body></html>"

    # Parse date for display
    try:
        date_obj = datetime.strptime(file_date, "%d%m%Y")
        display_date = date_obj.strftime("%B %d, %Y")
    except Exception:
        display_date = file_date

    # Get unique groups
    groups = df['Group'].unique()

    # Create individual graphs for each group
    graphs_html = []

    for group in groups:
        group_data = df[df['Group'] == group].copy()

        # Sort by 1-week performance
        group_data = group_data.sort_values('1_Week_Performance_%', ascending=True)

        # Take top 15 and bottom 10 if more than 25 ETFs
        if len(group_data) > 25:
            top_15 = group_data.tail(15)
            bottom_10 = group_data.head(10)
            group_data = pd.concat([bottom_10, top_15])

        # Prepare customdata with ETF_Name (AUM not present in India file)
        customdata = np.column_stack((group_data['ETF_Name'],))

        # Create horizontal bar chart for performance comparison
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                f'{group} - 1 Week Performance',
                f'{group} - 1 Month Performance',
                f'{group} - 6 Month Performance',
                f'{group} - YTD Performance'
            ),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )

        # 1-Week Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['1_Week_Performance_%'],
                orientation='h',
                name='1 Week',
                marker=dict(color=group_data['1_Week_Performance_%'],
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['1_Week_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=1, col=1
        )

        # 1-Month Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['1_Month_Performance_%'],
                orientation='h',
                name='1 Month',
                marker=dict(color=group_data['1_Month_Performance_%'],
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['1_Month_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=1, col=2
        )

        # 6-Month Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['6_Month_Performance_%'],
                orientation='h',
                name='6 Month',
                marker=dict(color=group_data['6_Month_Performance_%'],
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['6_Month_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=2, col=1
        )

        # YTD Performance
        fig.add_trace(
            go.Bar(
                y=group_data['ETF_Ticker'],
                x=group_data['YTD_Performance_%'],
                orientation='h',
                name='YTD',
                marker=dict(color=group_data['YTD_Performance_%'],
                           colorscale='RdYlGn',
                           showscale=False),
                text=group_data['YTD_Performance_%'].round(2),
                textposition='outside',
                customdata=customdata,
                hovertemplate='<b>%{y}</b><br>%{customdata[0]}<br>Performance: %{x:.2f}%<extra></extra>'
            ),
            row=2, col=2
        )

        # Update layout
        fig.update_xaxes(title_text="Performance (%)", row=1, col=1)
        fig.update_xaxes(title_text="Performance (%)", row=1, col=2)
        fig.update_xaxes(title_text="Performance (%)", row=2, col=1)
        fig.update_xaxes(title_text="Performance (%)", row=2, col=2)

        fig.update_layout(
            height=800,
            showlegend=False,
            title_text=f"{group} ETFs Performance Analysis",
            title_x=0.5,
            title_font_size=20
        )

        graphs_html.append(fig.to_html(full_html=False, include_plotlyjs='cdn'))

        # Add trend distribution pie chart
        trend_fig = go.Figure()

        trend_counts = group_data['Trend_SMA'].value_counts()

        trend_fig.add_trace(go.Pie(
            labels=trend_counts.index,
            values=trend_counts.values,
            hole=0.3,
            marker=dict(colors=['#2ecc71', '#e74c3c', '#95a5a6', '#34495e'])
        ))

        trend_fig.update_layout(
            title_text=f"{group} ETFs - Trend Distribution (SMA Method)",
            title_x=0.5,
            height=400
        )

        graphs_html.append(trend_fig.to_html(full_html=False, include_plotlyjs='cdn'))

    # Summary statistics
    total_etfs = len(df)
    avg_1week = df['1_Week_Performance_%'].mean()
    avg_1month = df['1_Month_Performance_%'].mean()
    avg_ytd = df['YTD_Performance_%'].mean()

    # Top 5 performers
    top_5 = df.nlargest(5, '1_Week_Performance_%')[['ETF_Ticker', 'ETF_Name', 'Group', '1_Week_Performance_%', '1_Month_Performance_%']]

    # Create top performers table HTML
    top_performers_html = top_5.to_html(index=False, classes='table table-striped', border=0)

    # Combine all graphs
    all_graphs = '\n'.join(graphs_html)

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>India ETF Performance Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f5f5;
                padding: 20px;
            }}
            .header {{
                background: linear-gradient(135deg, #0f9b0f 0%, #16a085 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }}
            .stats-card {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            .stat-value {{
                font-size: 2em;
                font-weight: bold;
                color: #0f9b0f;
            }}
            .stat-label {{
                color: #666;
                font-size: 0.9em;
            }}
            .graph-container {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }}
            .table {{
                background: white;
            }}
            h2 {{
                color: #333;
                margin-top: 30px;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container-fluid">
            <div class="header">
                <h1>🇮🇳 India ETF Performance Dashboard</h1>
                <p class="mb-0">Data as of: <strong>{display_date}</strong></p>
                <p class="mb-0">Total ETFs Analyzed: <strong>{total_etfs}</strong></p>
            </div>

            <div class="row">
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Average 1-Week Performance</div>
                        <div class="stat-value">{avg_1week:.2f}%</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Average 1-Month Performance</div>
                        <div class="stat-value">{avg_1month:.2f}%</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Average YTD Performance</div>
                        <div class="stat-value">{avg_ytd:.2f}%</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stats-card">
                        <div class="stat-label">Groups Analyzed</div>
                        <div class="stat-value">{len(groups)}</div>
                    </div>
                </div>
            </div>

            <div class="graph-container">
                <h2>📊 Top 5 Performers (1-Week)</h2>
                {top_performers_html}
            </div>

            <h2>📈 Performance by Group</h2>

            {all_graphs}

            <div class="footer text-center mt-5 mb-3">
                <p class="text-muted">
                    <a href="/api/etf-performance" class="btn btn-primary me-2">View US JSON API</a>
                    <a href="/docs" class="btn btn-secondary">API Documentation</a>
                </p>
                <p class="text-muted small">ETF Performance Service v1.0.0</p>
            </div>
        </div>
    </body>
    </html>
    """

    return html_content

if __name__ == "__main__":
    import uvicorn
    print("Starting ETF Performance Service...")
    print("Dashboard: http://localhost:8000/dashboard")
    print("API: http://localhost:8000/api/etf-performance")
    print("API Docs: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)
