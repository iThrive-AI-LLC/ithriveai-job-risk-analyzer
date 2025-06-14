"""
Career Navigator Module (career_navigator.py)

This module provides HTML content for the Career Navigator feature,
which is displayed as a call-to-action at the end of the job analysis results.
"""

def get_html() -> str:
    """
    Returns a string containing the HTML for the Career Navigator section.
    The HTML includes styling and a link to a Jotform for users to get a
    personalized career plan.
    """
    html_content = """
    <div style="background-color: #0084FF; padding: 30px; border-radius: 10px; text-align: center; margin-top: 20px; margin-bottom: 20px;">
        <div style="background-color: white; padding: 12px; border-radius: 8px; margin-bottom: 20px; display: inline-block;">
            <h2 style="color: #0084FF; font-size: 24px; font-weight: bold; margin: 0;">Transform AI Risk Into Career Opportunity</h2>
        </div>
        <p style="color: white; font-size: 18px; margin-bottom: 20px; line-height: 1.6;">
            Don't just analyze your job risk - get a complete career transformation strategy with our Career Navigator service.
        </p>
        <div style="background-color: white; padding: 25px; border-radius: 8px; margin: 20px auto; max-width: 90%; color: #333333; text-align: left;">
            <h3 style="color: #0084FF; margin-top: 0; font-size: 20px; text-align: center; margin-bottom: 15px;">Transform Your Career With Expert Guidance:</h3>
            <ul style="list-style-type: disc; margin: 0 auto 20px auto; padding-left: 20px; max-width: 85%; font-size: 16px; line-height: 1.7;">
                <li><strong>Personalized Career Strength Profile</strong> - Discover your unique transferable skills that AI can't replace.</li>
                <li><strong>AI-Resilient Career Pathways</strong> - Custom-matched opportunities with detailed transition plans.</li>
                <li><strong>Curated Training Resources</strong> - Top 10 recommended courses with links and cost estimates.</li>
                <li><strong>Targeted Career Matches</strong> - Three ideal roles with descriptions and salary ranges.</li>
                <li><strong>Strategic Learning Roadmap</strong> - Exact certifications and courses with the highest career ROI.</li>
                <li><strong>Month-by-Month Action Plan</strong> - Clear steps to secure more valuable, future-proof positions.</li>
            </ul>
        </div>
        <a href="https://form.jotform.com/251137815706154" target="_blank" 
           style="display: inline-block; background-color: white; color: #0084FF; padding: 15px 30px; 
                  text-decoration: none; border-radius: 5px; font-weight: bold; font-size: 18px; 
                  margin-top: 15px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); transition: background-color 0.3s, transform 0.2s;"
           onmouseover="this.style.backgroundColor='#e6f3ff'; this.style.transform='translateY(-2px)';"
           onmouseout="this.style.backgroundColor='white'; this.style.transform='translateY(0px)';">
            Get Your Career Navigator Package
        </a>
        <p style="color: white; font-size: 14px; margin-top: 20px;">
            Join professionals who've secured higher-paying, AI-resilient positions with our strategic guidance.
        </p>
    </div>
    """
    return html_content

if __name__ == "__main__":
    # This part is for testing the module directly.
    # It won't run when imported by the Streamlit app.
    print("Testing career_navigator.py...")
    html_output = get_html()
    print("\nGenerated HTML Output:\n")
    print(html_output)
    
    # You can save this to an HTML file to preview in a browser:
    try:
        with open("career_navigator_preview.html", "w", encoding="utf-8") as f:
            f.write("<html><head><title>Career Navigator Preview</title></head><body style='font-family: sans-serif; padding: 20px;'>")
            f.write(html_output)
            f.write("</body></html>")
        print("\nPreview HTML saved to career_navigator_preview.html")
    except IOError as e:
        print(f"\nError saving preview HTML: {e}")
