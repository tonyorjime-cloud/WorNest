
import os
from fpdf import FPDF

def generate_biweekly_pdf(data, photos, output_path):
    pdf = FPDF()
    pdf.add_page()

    pdf.set_font("Arial", "B", 14)
    pdf.cell(0,10,"FCDA – PUBLIC BUILDINGS DEPARTMENT", ln=True)
    pdf.cell(0,10,"STRUCTURES BRANCH", ln=True)
    pdf.cell(0,10,"BIWEEKLY SITE SUPERVISION REPORT", ln=True)

    pdf.ln(5)
    pdf.set_font("Arial","",11)

    for k,v in data.items():
        pdf.cell(0,8,f"{k}: {v}", ln=True)

    pdf.ln(5)
    pdf.cell(0,8,"PHOTOS", ln=True)

    for p in photos:
        if os.path.exists(p):
            pdf.image(p, w=150)
            pdf.ln(5)

    pdf.output(output_path)
