import gradio as gr
import os

class Log:
    """
    A simple component for Gradio UI to display log files
    """
    def __init__(self, log_file, dark=False, xterm_font_size=14):
        self.log_file = log_file
        self.dark = dark
        self.font_size = xterm_font_size
        
        # Create empty log file if it doesn't exist
        if not os.path.exists(log_file):
            with open(log_file, 'w') as f:
                pass
                
        # Define HTML for log display with auto-refresh
        refresh_interval = 2000  # 2 seconds
        theme = "dark" if dark else "light"
        
        html_content = f"""
        <div id="log-container" style="height: 400px; overflow-y: auto; font-family: monospace; 
                                       background-color: {'#000' if dark else '#f5f5f5'}; 
                                       color: {'#fff' if dark else '#333'}; 
                                       padding: 10px; border-radius: 5px; 
                                       font-size: {xterm_font_size}px;">
            <pre id="log-content"></pre>
        </div>
        
        <script>
            function fetchLog() {{
                fetch('file={log_file}')
                    .then(response => response.text())
                    .then(data => {{
                        document.getElementById('log-content').textContent = data;
                        const container = document.getElementById('log-container');
                        container.scrollTop = container.scrollHeight;
                    }})
                    .catch(error => console.error('Error fetching log:', error));
            }}
            
            // Initial fetch
            fetchLog();
            
            // Refresh every {refresh_interval}ms
            setInterval(fetchLog, {refresh_interval});
        </script>
        """
        
        gr.HTML(html_content) 