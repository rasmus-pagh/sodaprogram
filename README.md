The script produces an HTML file with an overview of conference talks of the SODA conference plus affiliated conferences ALENEX and SOSA. Probably it can be adapted to work with other SIAM conferences. The script is run as follows:
```
python sodaprogram.py "<url>"
```
where `<url>` is replaced with the URL of the official SIAM conference schedule, e.g., `https://meetings.siam.org/program.cfm?CONFCODE=SODA25`

The output is a single file `conference_program_<year>.html` that contains a schedule with links to the official page for each session and each presentation.
