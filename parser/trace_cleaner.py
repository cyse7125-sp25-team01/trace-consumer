import re

def extract_metadata_from_filename(filename):
    """
    Extract metadata from standardized filename pattern.
    Example: Parikh_Tejas_000937178_Spring-2024_CSYE622503Lecture_Instructor-Report.pdf
    """
    pattern = r'([A-Za-z]+)_([A-Za-z]+)_(\d+)_([A-Za-z]+)-(\d{4})_([A-Za-z0-9]+)_([A-Za-z-]+).pdf'
    match = re.match(pattern, filename)
    if match:
        last_name, first_name, id_number, semester, year, course_code, report_type = match.groups()
        return {
            'instructor_last_name': last_name,
            'instructor_first_name': first_name,
            'instructor_id': id_number,
            'semester': semester,
            'year': year,
            'course_code': course_code,
            'report_type': report_type.replace('-', ' ')
        }
    return {}

def clean_evaluation_text(text):
    results = {
        'full_text': '',
        'course_info': {},
        'ratings': [],
        'comments': []
    }

    text = re.sub(r'\s+', ' ', text)

    course_name_match = re.search(r'([A-Za-z0-9\s:&\-]+)(?=\s+\((Spring|Fall)\s+\d{4}\))', text)
    if course_name_match:
        results['course_info']['course_name'] = course_name_match.group(1).strip()

    instructor_match = re.search(r'Instructor:\s*([^\n\r]+)', text)
    if instructor_match:
        name_raw = instructor_match.group(1).strip()

        # Trim if subject or other fields are accidentally pulled in
        name_raw = re.split(r'Subject:|Catalog & Section:|Enrollment:', name_raw)[0].strip()

        # Normalize "Last, First" to "First Last"
        if "," in name_raw:
            last, first = name_raw.split(",", 1)
            instructor_name = f"{first.strip()} {last.strip()}"
        else:
            instructor_name = name_raw

        results['course_info']['instructor'] = instructor_name

    subject_match = re.search(r'Subject:\s*(\w+)', text)
    if subject_match:
        results['course_info']['subject'] = subject_match.group(1).strip()

    catalog_match = re.search(r'Catalog & Section:\s*(\w+\s+\d+)', text)
    if catalog_match:
        results['course_info']['catalog_section'] = catalog_match.group(1).strip()

    enrollment_match = re.search(r'Enrollment:\s*(\d+)', text)
    if enrollment_match:
        results['course_info']['enrollment'] = int(enrollment_match.group(1))

    responses_match = re.search(r'Responses\s+Inc\w*\s+Declines:\s*(\d+)', text)
    if responses_match:
        results['course_info']['responses'] = int(responses_match.group(1))

    declines_match = re.search(r'Declines:\s*(\d+)', text)
    if declines_match:
        results['course_info']['declines'] = int(declines_match.group(1))

    comment_sections = re.findall(r'Q:\s+(What.*?|Please.*?)\n((?:\d+\s+.*?\n)+)', text, re.DOTALL)
    for question, comments_block in comment_sections:
        question = question.strip()
        individual_comments = re.findall(r'(\d+)\s+(.*?)(?=\n\d+\s+|\Z)', comments_block + '\n', re.DOTALL)
        for comment_num, comment_text in individual_comments:
            results['comments'].append({
                'question': question,
                'comment_number': int(comment_num),
                'text': comment_text.strip()
            })

    clean_text = re.sub(r'Netwrk Strctrs Cloud Cmpting \(Spring \d{4}\).*?Declines:', '', text, flags=re.DOTALL)
    clean_text = re.sub(r'(Question|Number of Responses|Response Rate|Course Mean|Dept\. Mean|Univ\. Mean|Course Median|Dept\. Median|Univ\. Median)\s+', '', clean_text)
    clean_text = re.sub(r'Note: 5:.*?;', '', clean_text)
    clean_text = re.sub(r'\n\s*\d+\s*\n', '\n', clean_text)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()

    results['full_text'] = clean_text
    return results

def extract_ratings_data(text):
    ratings = []
    sections = re.findall(
        r'((?:Questions to Assess|Course Related|Learning Related|Instructor Related).*?)\n(.*?)(?=Questions to Assess|Course Related|Learning Related|Instructor Related|\Z)',
        text, re.DOTALL)

    for section_title, section_content in sections:
        section_title = section_title.strip()
        rating_rows = re.findall(r'([A-Za-z].*?)\s+(\d+)\s+(\d+%)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)',
                                 section_content)
        for row in rating_rows:
            try:
                ratings.append({
                    'category': section_title,
                    'question': row[0].strip(),
                    'response_count': int(row[1]),
                    'response_rate': row[2],
                    'course_mean': float(row[3]),
                    'dept_mean': float(row[4]),
                    'univ_mean': float(row[5])
                })
            except (ValueError, IndexError):
                continue

    return ratings

def process_pdf_text(text):
    cleaned_results = clean_evaluation_text(text)
    cleaned_results['ratings'] = extract_ratings_data(text)
    return cleaned_results
