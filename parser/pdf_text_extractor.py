import fitz

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using PyMuPDF"""
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text
