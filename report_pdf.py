import os
from fpdf import FPDF

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp'}


def _safe(v):
    return '' if v is None else str(v)


def _norm_text(v):
    txt = _safe(v).replace('\r', '')
    return txt if txt.strip() else '—'


class ReportPDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 13)
        self.cell(0, 7, 'FCDA - PUBLIC BUILDINGS DEPARTMENT', new_x='LMARGIN', new_y='NEXT', align='C')
        self.cell(0, 7, 'STRUCTURES BRANCH', new_x='LMARGIN', new_y='NEXT', align='C')
        self.cell(0, 8, 'BIWEEKLY SITE SUPERVISION REPORT', new_x='LMARGIN', new_y='NEXT', align='C')
        self.ln(2)

    def footer(self):
        self.set_y(-12)
        self.set_font('Helvetica', '', 8)
        self.cell(0, 5, f'Page {self.page_no()}', align='C')

    def section(self, title, body):
        self.set_font('Helvetica', 'B', 11)
        self.set_fill_color(240, 240, 240)
        self.cell(0, 8, title, new_x='LMARGIN', new_y='NEXT', fill=True)
        self.set_font('Helvetica', '', 10)
        self.multi_cell(0, 6, _norm_text(body))
        self.ln(1)


def build_biweekly_report_pdf(project: dict, report: dict, attachments: list, prepared_by: str = '', approved_by: str = ''):
    pdf = ReportPDF(orientation='P', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.set_margins(12, 14, 12)
    pdf.add_page()

    pdf.set_font('Helvetica', '', 10)
    meta = [
        ('Project', f"{_safe(project.get('code'))} - {_safe(project.get('name'))}".strip(' -')),
        ('Location', _safe(project.get('location')) or '—'),
        ('Client', _safe(project.get('client')) or '—'),
        ('Report No.', _safe(report.get('cycle_no')) or '—'),
        ('Reporting Period', f"{_safe(report.get('window_start')) or _safe(report.get('report_date'))} -> {_safe(report.get('window_end')) or _safe(report.get('report_date'))}"),
        ('Submission Date', _safe(report.get('submitted_on')) or _safe(report.get('uploaded_at')) or '—'),
        ('Status', _safe(report.get('status')) or '—'),
        ('Timing', _safe(report.get('timing_status')) or '—'),
        ('Prepared By', prepared_by or '—'),
    ]
    if approved_by or report.get('approved_at'):
        meta.append(('Approved', f"{approved_by or '—'}  {_safe(report.get('approved_at'))}".strip()))

    for label, val in meta:
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(40, 6, f'{label}:')
        pdf.set_font('Helvetica', '', 10)
        pdf.multi_cell(0, 6, _norm_text(val))

    pdf.ln(1)
    pdf.section('Site Activities', report.get('site_activities'))
    pdf.section('Reinforcement Observations', report.get('reinforcement_observations'))
    pdf.section('Concrete / Test Observations', report.get('concrete_observations'))
    pdf.section('HSE Observations', report.get('hse_observations'))
    pdf.section('RFI / EI Notes', report.get('rfi_notes'))
    pdf.section('General Remarks', report.get('general_remarks'))

    image_rows = []
    for att in attachments or []:
        fp = _safe(att.get('file_path'))
        ext = os.path.splitext(fp)[1].lower()
        if fp and os.path.exists(fp) and ext in IMAGE_EXTS:
            image_rows.append({'file_path': fp, 'caption': _safe(att.get('caption'))})

    if image_rows:
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(0, 8, 'Site Photographs', new_x='LMARGIN', new_y='NEXT', fill=True)
        page_w = pdf.w - pdf.l_margin - pdf.r_margin
        col_w = (page_w - 6) / 2
        img_h = 55
        x_left = pdf.l_margin
        x_right = pdf.l_margin + col_w + 6
        current_x = x_left
        row_y = pdf.get_y()
        row_max_y = row_y

        for idx, item in enumerate(image_rows, start=1):
            if row_y + img_h + 18 > 280:
                pdf.add_page()
                row_y = pdf.get_y()
                row_max_y = row_y
                current_x = x_left

            pdf.set_xy(current_x, row_y)
            try:
                pdf.image(item['file_path'], x=current_x, y=row_y, w=col_w, h=img_h, keep_aspect_ratio=True)
            except Exception:
                pdf.rect(current_x, row_y, col_w, img_h)
                pdf.set_xy(current_x + 2, row_y + img_h / 2)
                pdf.set_font('Helvetica', '', 9)
                pdf.cell(col_w - 4, 5, 'Image could not be rendered', align='C')

            cap_y = row_y + img_h + 2
            pdf.set_xy(current_x, cap_y)
            pdf.set_font('Helvetica', '', 9)
            pdf.multi_cell(col_w, 4.5, _norm_text(item.get('caption') or f'Photo {idx}'))
            row_max_y = max(row_max_y, pdf.get_y())

            if current_x == x_left:
                current_x = x_right
            else:
                current_x = x_left
                row_y = row_max_y + 4
                pdf.set_y(row_y)

        pdf.set_y(max(pdf.get_y(), row_max_y + 2))

    out = pdf.output(dest='S')
    if isinstance(out, bytearray):
        return bytes(out)
    if isinstance(out, bytes):
        return out
    return out.encode('latin-1')
