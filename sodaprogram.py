import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from urllib.parse import urljoin
import concurrent.futures
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from datetime import datetime
import sys

output_folder = 'html/'

# Configure logging to capture debug information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Setup a session with retries
http_session = requests.Session()
retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
http_session.mount('http://', adapter)
http_session.mount('https://', adapter)

def extract_time(text):
    """
    Extract start and end times from the text.
    Returns a tuple (start_time_str, end_time_str) or (None, None) if not found.
    """
    # Try to extract a time range (e.g., '9:00 AM - 11:05 AM')
    match = re.search(r'(\d{1,2}:\d{2}\s?(AM|PM))\s*[-–—]\s*(\d{1,2}:\d{2}\s?(AM|PM))', text, re.IGNORECASE)
    if match:
        return (match.group(1), match.group(3))
    # If no range, try to extract a single time (e.g., '5:00 PM')
    match = re.search(r'\d{1,2}:\d{2}\s?(AM|PM)', text, re.IGNORECASE)
    if match:
        return (match.group(0), None)
    return (None, None)

def clean_session_title(title):
    """
    Remove 'CP1', 'CP2', etc., from the session title.
    """
    cleaned_title = re.sub(r'^CP\d+\s+', '', title).strip()
    return cleaned_title

def is_talk_session(session_title):
    """
    Determine if a session is a talk session based on its title.
    Returns True if it is a talk session, False otherwise.
    """
    # Define keywords that indicate a talk session
    talk_keywords = ['Session', 'IP', 'CP', 'SODA', 'ALENEX', 'SOSA', 'Workshop', 'Lecture']
    return any(keyword.lower() in session_title.lower() for keyword in talk_keywords)

def fetch_main_page(url):
    """Fetch the main conference page and parse session information."""
    try:
        response = http_session.get(url)
        response.raise_for_status()
        logging.info(f"Successfully fetched main page: {url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch the main page: {e}")
        return defaultdict(lambda: defaultdict(list)), "Conference Program"  # Default title if fetching fails
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract the main heading for dynamic header
    main_heading_tag = soup.find(['h1', 'h2'])
    main_heading = main_heading_tag.get_text(strip=True) if main_heading_tag else "Conference Program"
    logging.info(f"Extracted main heading: {main_heading}")
    
    sessions_by_day = defaultdict(list)  # day -> list of sessions with start_time, end_time, title, link, location
    current_day = None
    
    # Regular expression to detect day names
    day_pattern = re.compile(r'\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b', re.IGNORECASE)
    
    # Iterate through all table rows
    for row in soup.find_all('tr'):
        # Detect day headers: <tr><td colspan="...">Day Name</td></tr>
        td_day = row.find('td', colspan=True)
        if td_day:
            text = td_day.get_text(separator=' ', strip=True)
            if day_pattern.search(text):
                current_day = text
                logging.info(f"Detected new day: {current_day}")
            continue  # Move to the next row after setting the day
        
        # Parse session rows
        tds = row.find_all('td')
        if not tds:
            continue  # Skip if no <td> found
        
        # Determine if it's a new time slot or an additional concurrent session
        if len(tds) == 3:
            # New time slot
            time_text = tds[0].get_text(separator=' ', strip=True)
            start_time, end_time = extract_time(time_text)
            if not start_time:
                logging.debug(f"Could not extract time from text: {time_text}")
                continue  # Skip this row if time is not found
            
            # Extract session details
            session_cell = tds[1]
            session_link_tag = session_cell.find('a', href=True)
            if session_link_tag:
                session_title = session_link_tag.get_text(strip=True)
                session_title = clean_session_title(session_title)
                session_link = urljoin(url, session_link_tag['href'])
                
                # Extract location
                location = tds[2].get_text(separator=' ', strip=True)
                
                # Filter out non-talk sessions
                if not is_talk_session(session_title):
                    logging.debug(f"Omitting non-talk session: '{session_title}'")
                    continue
                
                # For debugging: log all parsed sessions
                logging.debug(f"Parsed session: {current_day}, {start_time}-{end_time}, {session_title}")
                
                sessions_by_day[current_day].append({
                    "start_time": start_time,
                    "end_time": end_time,
                    "title": session_title,
                    "link": session_link,
                    "location": location
                })
        
        elif len(tds) == 2:
            # Additional concurrent session for the current time slot
            if not current_day:
                logging.debug("Additional session row without a current day. Skipping.")
                continue  # Cannot associate session without a day
            
            # Extract session details
            session_cell = tds[0]
            session_link_tag = session_cell.find('a', href=True)
            if session_link_tag:
                session_title = session_link_tag.get_text(strip=True)
                session_title = clean_session_title(session_title)
                session_link = urljoin(url, session_link_tag['href'])
            else:
                session_title = session_cell.get_text(separator=' ', strip=True)
                session_link = "#"
                logging.debug(f"No link found for session '{session_title}'. Using default '#'.")
            
            # Extract location (assuming same as the last session)
            if sessions_by_day[current_day]:
                last_session = sessions_by_day[current_day][-1]
                location = last_session.get("location", "")
                start_time = last_session["start_time"]
                end_time = last_session["end_time"]
            else:
                location = ""
                start_time, end_time = extract_time(time_text)  # Fallback to current time slot if available
            
            # Filter out non-talk sessions
            if not is_talk_session(session_title):
                logging.debug(f"Omitting non-talk session: '{session_title}'")
                continue
            
            # For debugging: log all parsed sessions
            logging.debug(f"Parsed concurrent session: {current_day}, {start_time}-{end_time}, {session_title}")
            
            sessions_by_day[current_day].append({
                "start_time": start_time,
                "end_time": end_time,
                "title": session_title,
                "link": session_link,
                "location": location
            })
        else:
            logging.debug(f"Skipping row with unexpected number of <td>: {len(tds)}")
            continue  # Skip rows that do not match expected structures
    
    # Group sessions by day and then by start time
    grouped_sessions = defaultdict(lambda: defaultdict(list))  # day -> start_time -> list of sessions
    for day, sessions in sessions_by_day.items():
        for conf_session in sessions:
            grouped_sessions[day][conf_session["start_time"]].append(conf_session)
    
    logging.info(f"Total days parsed: {len(grouped_sessions)}")
    for day, times in grouped_sessions.items():
        logging.info(f"Day: {day}, Number of time slots: {len(times)}")
        for time, sessions in times.items():
            logging.info(f"  Time: {time}, Number of sessions: {len(sessions)}")
    
    return grouped_sessions, main_heading

def fetch_session_details(session_url, session_title):
    """Fetch detailed talk information from session page."""
    talks = []
    if not session_url.startswith("https://meetings.siam.org/sess"):
        return talks # Return empty list for non-sessions
    
    try:
        response = http_session.get(session_url)
        response.raise_for_status()
        logging.debug(f"Successfully fetched session page: {session_url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch session page '{session_title}': {e}")
        return talks  # Return empty list if fetching fails
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Assuming talks are listed within <dt> tags; adjust if necessary
    for talk in soup.find_all('dt'):
        title_tag = talk.find('strong')
        title = title_tag.get_text(strip=True) if title_tag else "Unknown Title"
        abstract_link = talk.find('a', href=True)
        talk_link = urljoin(session_url, abstract_link['href']) if abstract_link else "#"
        talks.append({"title": title, "link": talk_link})
    
    # For debugging: log the number of talks fetched
    logging.debug(f"Fetched {len(talks)} talks for session: {session_title}")
    
    return talks

def fetch_all_session_details(grouped_sessions):
    """Fetch details for all sessions concurrently."""
    all_talks_by_day = defaultdict(lambda: defaultdict(list))  # day -> time_range -> list of sessions with talks
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_session = {}
        for day, times in grouped_sessions.items():
            for start_time, sessions in times.items():
                # Determine the overall time range for this group of sessions
                end_times = [s["end_time"] for s in sessions if s["end_time"]]
                if end_times:
                    # Convert end times to datetime objects for comparison
                    try:
                        end_times_dt = [datetime.strptime(et, '%I:%M %p') for et in end_times]
                        max_end_time_dt = max(end_times_dt)
                        max_end_time = max_end_time_dt.strftime('%I:%M %p').lstrip('0')
                        time_range = f"{start_time} - {max_end_time}"
                    except ValueError as ve:
                        logging.error(f"Time format error: {ve}")
                        time_range = start_time  # Fallback to start_time if parsing fails
                else:
                    time_range = start_time
                
                for conf_session in sessions:
                    future = executor.submit(fetch_session_details, conf_session["link"], conf_session["title"])
                    future_to_session[future] = (day, time_range, conf_session)
        
        for future in concurrent.futures.as_completed(future_to_session):
            day, time_range, conf_session = future_to_session[future]
            try:
                talks = future.result()
                all_talks_by_day[day][time_range].append({
                    "title": conf_session["title"],
                    "link": conf_session["link"],
                    "talks": talks
                })
                logging.debug(f"Added session '{conf_session['title']}' with {len(talks)} talks to '{day}' at '{time_range}'.")
            except Exception as e:
                logging.error(f"Error processing session '{conf_session['title']}' on {day} at {time_range}: {e}")
    
    logging.info(f"Total sessions with talks fetched: {sum(len(sessions) for day in all_talks_by_day for sessions in all_talks_by_day[day].values())}")
    return all_talks_by_day

def generate_html(all_talks_by_day, main_heading, max_concurrent_sessions=4, url=None):
    """Generate HTML output with aligned concurrent talks by time slots."""
    # Start building the HTML content with dynamic main heading
    html_content = f'''
    <html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
        <title>{main_heading}</title>
        <style>
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ border: 1px solid #ccc; padding: 10px; vertical-align: top; }}
            th {{ background-color: #f4f4f4; text-align: center; }}
            a {{ text-decoration: none; color: black; }}
            .day-header {{ background-color: #d9edf7; font-size: 1.2em; }}
            .session-title {{ font-weight: bold; }}
            a:link {{
              color: black;
            }}
        </style>
    </head>
    <body>
        <h1>{main_heading}</h1>
    '''
    if url is not None:
        todaysdate = datetime.now().isoformat()[:10]
        html_content += f'<p>Generated {todaysdate} from the official program (<a href="{url}">link</a>)</p>'
    html_content += '''    
        <table>
            <tbody>
    '''
    
    # Sort days based on the date
    for day in sorted(all_talks_by_day.keys(), key=lambda d: datetime.strptime(d.split(', ')[1], '%B %d')):
        # Day Header Row
        html_content += f'''
            <tr>
                <th class="day-header" colspan="{max_concurrent_sessions}">{day}</th>
            </tr>
        '''
        # Sort time slots chronologically based on start time
        sorted_time_ranges = sorted(all_talks_by_day[day].keys(), key=lambda x: datetime.strptime(x.split(' - ')[0], '%I:%M %p'))
        for time_range in sorted_time_ranges:
            sessions = all_talks_by_day[day][time_range]
            if len(sessions) == 1:
                # Single session: span all session columns
                session = sessions[0]
                session_title = session["title"]
                session_link = session["link"]
                talks = session["talks"]
                # Incorporate the time range within the session cell
                session_cell = f'<strong class="session-title"><a href="{session_link}">{session_title}</a></strong><br>'
                if len(talks) == 0: # Add time range in italics
                    session_cell += f'<em>{time_range}</em><br>'
                for talk in talks:
                    session_cell += f'<a href="{talk["link"]}">{talk["title"]}</a><br>'
                # Add the row without the Time column
                html_content += f'''
                    <tr>
                        <td colspan="{max_concurrent_sessions}">{session_cell}</td>
                    </tr>
                '''
            else:
                # Multiple concurrent sessions: up to max_concurrent_sessions
                html_content += f'''
                    <tr>
                '''
                sessions = sorted(sessions, key=lambda d: d['title'].replace('ALENEX','Z'))
                for i in range(max_concurrent_sessions):
                    if i < len(sessions):
                        session = sessions[i]
                        session_title = session["title"]
                        session_link = session["link"]
                        talks = session["talks"]
                        session_cell = f'<strong class="session-title"><a href="{session_link}">{session_title}</a></strong><br>'
                        for talk in talks:
                            session_cell += f'<a href="{talk["link"]}">{talk["title"]}</a><br>'
                        html_content += f'''
                            <td>
                                {session_cell}
                            </td>
                        '''
                    else:
                        # Empty cell for missing concurrent sessions
                        html_content += '''
                            <td></td>
                        '''
                html_content += '''
                    </tr>
                '''
        
    # Close the table and HTML tags
    html_content += '''
            </tbody>
        </table>
    </body>
    </html>
    '''
    return html_content

def determine_max_concurrent_sessions(all_talks_by_day):
    """Determine the maximum number of concurrent sessions across all days and times."""
    max_sessions = 0
    for day, times in all_talks_by_day.items():
        for time_range, sessions in times.items():
            count = len(sessions)
            if count > max_sessions:
                max_sessions = count
    logging.info(f"Maximum concurrent sessions across all days: {max_sessions}")
    return max_sessions

# Main execution
def main():
    if len(sys.argv) < 2:
        print("Usage: python sodaprogram.py \"<url>\"")
        sys.exit(0)
    url = sys.argv[1] # E.g. https://meetings.siam.org/program.cfm?CONFCODE=SODA25
    logging.info("Fetching main conference page...")
    grouped_sessions, main_heading = fetch_main_page(url)
    
    if not grouped_sessions:
        logging.error("No sessions were parsed. Please check the parsing logic and HTML structure.")
        return
    
    logging.info("Fetching session details...")
    all_talks_by_day = fetch_all_session_details(grouped_sessions)
    
    if not all_talks_by_day:
        logging.error("No session details were fetched. Please check the session URLs and fetching logic.")
        return
    
    # Determine the maximum number of concurrent sessions
    max_sessions = determine_max_concurrent_sessions(all_talks_by_day)
    logging.info(f"Maximum concurrent sessions identified: {max_sessions}")
    
    logging.info("Generating HTML output...")
    html_output = generate_html(all_talks_by_day, main_heading, max_sessions, url)
    
    year = main_heading[:4]
    output_file = f'{output_folder}conference_program_{year}.html'
    try:
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(html_output)
        logging.info(f"HTML output successfully saved to {output_file}")
    except IOError as e:
        logging.error(f"Failed to write HTML output to file: {e}")

if __name__ == "__main__":
    main()
