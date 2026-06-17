"""
Generates 4 PDF documents from dbt manifest.json:
  1. Lineage DAG (visual graph)
  2. Model Descriptions (schema.yml + SCD version control)
  3. Tests per model
  4. Registered Sources

Run from repo root:
  python3 scripts/generate_dbt_docs_pdfs.py
"""

import json
import os
import textwrap
from collections import defaultdict
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape, A3
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Image, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

# ── Config ────────────────────────────────────────────────────────────────────
MANIFEST_PATH = "target/manifest.json"
OUTPUT_DIR    = "dbt_docs_export"
GENERATED_AT  = datetime.now().strftime("%Y-%m-%d %H:%M")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Load manifest ─────────────────────────────────────────────────────────────
with open(MANIFEST_PATH) as f:
    manifest = json.load(f)

all_nodes   = manifest.get("nodes", {})
all_sources = manifest.get("sources", {})

models    = {k: v for k, v in all_nodes.items() if v["resource_type"] == "model"}
tests_raw = {k: v for k, v in all_nodes.items() if v["resource_type"] == "test"}

# ── Domain colors ─────────────────────────────────────────────────────────────
DOMAIN_COLORS = {
    "sonnell":          "#4C72B0",
    "ama":              "#55A868",
    "hms":              "#C44E52",
    "monitoring":       "#8172B2",
    "ati_datawarehouse":"#CCB974",
    "source":           "#64B5CD",
    "other":            "#AAAAAA",
}

def get_domain(node):
    fqn = node.get("fqn", [])
    if len(fqn) > 1:
        d = fqn[1].lower()
        for key in DOMAIN_COLORS:
            if key in d:
                return key
    return "other"

def domain_color(node):
    return DOMAIN_COLORS.get(get_domain(node), "#AAAAAA")

def wrap(text, width=90):
    if not text:
        return ""
    return "\n".join(textwrap.wrap(text.strip(), width))

# ── Styles ────────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

TITLE   = ParagraphStyle("title",   parent=styles["Title"],  fontSize=22, spaceAfter=6,  textColor=colors.HexColor("#1a2f5e"))
H1      = ParagraphStyle("h1",      parent=styles["Heading1"],fontSize=15, spaceAfter=4,  textColor=colors.HexColor("#1a2f5e"))
H2      = ParagraphStyle("h2",      parent=styles["Heading2"],fontSize=12, spaceAfter=3,  textColor=colors.HexColor("#2a4a8a"), spaceBefore=8)
H3      = ParagraphStyle("h3",      parent=styles["Heading3"],fontSize=10, spaceAfter=2,  textColor=colors.HexColor("#444444"), spaceBefore=5)
BODY    = ParagraphStyle("body",    parent=styles["Normal"],  fontSize=9,  spaceAfter=3,  leading=13)
SMALL   = ParagraphStyle("small",   parent=styles["Normal"],  fontSize=8,  textColor=colors.HexColor("#555555"))
CODE    = ParagraphStyle("code",    parent=styles["Code"],    fontSize=8,  backColor=colors.HexColor("#f5f5f5"), leading=11)
LABEL   = ParagraphStyle("label",   parent=styles["Normal"],  fontSize=8,  textColor=colors.white, backColor=colors.HexColor("#4C72B0"))
META    = ParagraphStyle("meta",    parent=styles["Normal"],  fontSize=8,  textColor=colors.HexColor("#777777"), alignment=TA_RIGHT)

TABLE_HEADER = colors.HexColor("#1a2f5e")
TABLE_ROW_A  = colors.HexColor("#EEF2FF")
TABLE_ROW_B  = colors.white

def page_header(canvas, doc, title):
    canvas.saveState()
    canvas.setFillColor(colors.HexColor("#1a2f5e"))
    canvas.rect(0, doc.pagesize[1] - 1.2*cm, doc.pagesize[0], 1.2*cm, fill=1, stroke=0)
    canvas.setFont("Helvetica-Bold", 10)
    canvas.setFillColor(colors.white)
    canvas.drawString(1.5*cm, doc.pagesize[1] - 0.85*cm, f"wh_transform — {title}")
    canvas.drawRightString(doc.pagesize[0] - 1.5*cm, doc.pagesize[1] - 0.85*cm, f"Generado: {GENERATED_AT}")
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#888888"))
    canvas.drawCentredString(doc.pagesize[0]/2, 0.6*cm, f"Página {doc.page}")
    canvas.restoreState()


# ═══════════════════════════════════════════════════════════════════════════════
# PDF 1 — LINEAGE DAG
# ═══════════════════════════════════════════════════════════════════════════════
def build_dag_image(output_path, domain_filter=None, figsize=(28, 18), title="Linaje Completo"):
    G = nx.DiGraph()
    node_colors_map = {}
    node_labels     = {}
    node_shapes     = {}

    # Add source nodes
    for uid, src in all_sources.items():
        short = f"src:{src['source_name']}\n{src['name']}"
        G.add_node(uid, label=short)
        node_colors_map[uid] = DOMAIN_COLORS["source"]
        node_shapes[uid] = "s"

    # Add model nodes
    for uid, m in models.items():
        if domain_filter and get_domain(m) not in domain_filter:
            continue
        G.add_node(uid, label=m["name"])
        node_colors_map[uid] = domain_color(m)
        node_shapes[uid] = "o"

    # Add edges
    for uid, m in models.items():
        if uid not in G.nodes:
            continue
        for dep in m.get("depends_on", {}).get("nodes", []):
            if dep in G.nodes:
                G.add_edge(dep, uid)
            elif dep in all_sources:
                if dep not in G.nodes:
                    src = all_sources[dep]
                    G.add_node(dep, label=f"src:{src['source_name']}\n{src['name']}")
                    node_colors_map[dep] = DOMAIN_COLORS["source"]
                G.add_edge(dep, uid)

    # Remove isolated source nodes that have no edges
    isolated = [n for n in list(G.nodes) if G.degree(n) == 0 and n.startswith("source.")]
    G.remove_nodes_from(isolated)

    if len(G.nodes) == 0:
        return None

    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_facecolor("#f8f9fc")
    ax.set_facecolor("#f8f9fc")

    try:
        pos = nx.nx_agraph.graphviz_layout(G, prog="dot")
    except Exception:
        pos = nx.spring_layout(G, k=2.5, iterations=60, seed=42)

    node_list   = list(G.nodes)
    color_list  = [node_colors_map.get(n, "#AAAAAA") for n in node_list]
    labels_dict = {n: G.nodes[n].get("label", n.split(".")[-1]) for n in node_list}

    nx.draw_networkx_edges(
        G, pos, ax=ax,
        arrows=True, arrowsize=12,
        edge_color="#CCCCCC", width=0.7,
        connectionstyle="arc3,rad=0.05",
        min_source_margin=10, min_target_margin=10
    )
    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        nodelist=node_list,
        node_color=color_list,
        node_size=1200, alpha=0.92
    )
    nx.draw_networkx_labels(
        G, pos, labels=labels_dict, ax=ax,
        font_size=5.5, font_color="white", font_weight="bold"
    )

    legend_patches = [
        mpatches.Patch(color=DOMAIN_COLORS["source"],           label="Fuente (source)"),
        mpatches.Patch(color=DOMAIN_COLORS["sonnell"],          label="Sonnell"),
        mpatches.Patch(color=DOMAIN_COLORS["ama"],              label="AMA / Geotab"),
        mpatches.Patch(color=DOMAIN_COLORS["hms"],              label="HMS"),
        mpatches.Patch(color=DOMAIN_COLORS["monitoring"],       label="Monitoring"),
        mpatches.Patch(color=DOMAIN_COLORS["ati_datawarehouse"],label="ATI DWH"),
        mpatches.Patch(color=DOMAIN_COLORS["other"],            label="Otros / Legacy"),
    ]
    ax.legend(handles=legend_patches, loc="lower left", fontsize=8,
              framealpha=0.9, edgecolor="#cccccc")
    ax.set_title(title, fontsize=14, fontweight="bold", color="#1a2f5e", pad=12)
    ax.axis("off")
    plt.tight_layout(pad=1.5)
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="#f8f9fc")
    plt.close()
    return output_path


def pdf_lineage():
    out_path = os.path.join(OUTPUT_DIR, "01_linaje_dag.pdf")
    doc = SimpleDocTemplate(
        out_path, pagesize=landscape(A3),
        leftMargin=1.5*cm, rightMargin=1.5*cm,
        topMargin=2*cm, bottomMargin=1.5*cm
    )

    def header(c, d): page_header(c, d, "Linaje — DAG Completo")

    story = []
    story.append(Paragraph("Linaje del Proyecto — DAG Completo", TITLE))
    story.append(Paragraph(
        f"<b>55 modelos</b> · <b>56 fuentes</b> · Dependencias completas entre capas "
        f"(source → staging → intermediate → mart)",
        BODY
    ))
    story.append(Spacer(1, 0.3*cm))

    # Stats table
    domain_counts = defaultdict(int)
    for m in models.values():
        domain_counts[get_domain(m)] += 1
    mat_counts = defaultdict(int)
    for m in models.values():
        mat_counts[m.get("config", {}).get("materialized", "?")] += 1

    stats_data = [["Dominio", "Modelos"]] + [[d.title(), str(c)] for d, c in sorted(domain_counts.items())]
    mat_data   = [["Materialización", "Cant."]] + [[k, str(v)] for k, v in sorted(mat_counts.items())]

    tbl_style = TableStyle([
        ("BACKGROUND", (0,0), (-1,0), TABLE_HEADER),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE",   (0,0), (-1,-1), 8),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [TABLE_ROW_A, TABLE_ROW_B]),
        ("GRID",       (0,0), (-1,-1), 0.4, colors.HexColor("#cccccc")),
        ("ALIGN",      (1,0), (1,-1), "CENTER"),
        ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING", (0,0),(-1,-1), 5),
        ("RIGHTPADDING",(0,0),(-1,-1), 5),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ])

    row1 = Table([[
        Table(stats_data, colWidths=[4*cm, 2.5*cm], style=tbl_style),
        Spacer(0.5*cm, 0),
        Table(mat_data,   colWidths=[4*cm, 2.5*cm], style=tbl_style),
    ]], colWidths=[7*cm, 0.5*cm, 7*cm])
    story.append(row1)
    story.append(Spacer(1, 0.4*cm))

    # Full DAG image
    dag_img = os.path.join(OUTPUT_DIR, "_dag_full.png")
    build_dag_image(dag_img, figsize=(30, 18), title="Linaje Completo — wh_transform")
    if dag_img and os.path.exists(dag_img):
        story.append(Image(dag_img, width=38*cm, height=22*cm))

    story.append(PageBreak())

    # Per-domain sub-DAGs
    for domain in ["sonnell", "ama", "hms", "monitoring", "ati_datawarehouse"]:
        story.append(Paragraph(f"Linaje: Dominio {domain.upper()}", H1))
        img_path = os.path.join(OUTPUT_DIR, f"_dag_{domain}.png")
        build_dag_image(img_path, domain_filter={domain, "other"},
                        figsize=(26, 14), title=f"Dominio: {domain.title()}")
        if os.path.exists(img_path):
            story.append(Image(img_path, width=36*cm, height=18*cm))
        story.append(PageBreak())

    doc.build(story, onFirstPage=header, onLaterPages=header)
    print(f"  ✓ {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# PDF 2 — MODEL DESCRIPTIONS
# ═══════════════════════════════════════════════════════════════════════════════
def pdf_model_descriptions():
    out_path = os.path.join(OUTPUT_DIR, "02_descripciones_modelos.pdf")
    doc = SimpleDocTemplate(
        out_path, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2.2*cm, bottomMargin=1.8*cm
    )
    def header(c, d): page_header(c, d, "Descripciones de Modelos")

    tbl_style = TableStyle([
        ("BACKGROUND", (0,0), (-1,0), TABLE_HEADER),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE",   (0,0), (-1,-1), 8),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [TABLE_ROW_A, TABLE_ROW_B]),
        ("GRID",       (0,0), (-1,-1), 0.4, colors.HexColor("#cccccc")),
        ("VALIGN",     (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (-1,-1), 5),
        ("RIGHTPADDING",(0,0), (-1,-1), 5),
        ("TOPPADDING",  (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ])

    story = []
    story.append(Paragraph("Documentación de Modelos dbt", TITLE))
    story.append(Paragraph(
        f"Proyecto <b>wh_transform</b> · {len(models)} modelos en {len(set(get_domain(m) for m in models.values()))} dominios · "
        f"Generado: {GENERATED_AT}",
        BODY
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#1a2f5e")))
    story.append(Spacer(1, 0.3*cm))

    # Group by domain
    domains_order = ["sonnell", "ama", "hms", "monitoring", "ati_datawarehouse", "other"]
    by_domain = defaultdict(list)
    for uid, m in models.items():
        by_domain[get_domain(m)].append(m)

    domain_labels = {
        "sonnell":          "Sonnell — Pipeline de Facturación",
        "ama":              "AMA — Dashboard Flota Geotab",
        "hms":              "HMS — Ferry Pipeline",
        "monitoring":       "Monitoring — Salud de Pipelines",
        "ati_datawarehouse":"ATI Datawarehouse — Migración Korbato",
        "other":            "Modelos Legacy / Ad-hoc",
    }

    for domain in domains_order:
        domain_models = sorted(by_domain.get(domain, []), key=lambda m: m["name"])
        if not domain_models:
            continue

        story.append(Paragraph(domain_labels.get(domain, domain.title()), H1))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor(DOMAIN_COLORS.get(domain, "#aaaaaa"))))
        story.append(Spacer(1, 0.15*cm))

        for m in domain_models:
            name  = m.get("name", "?")
            desc  = m.get("description", "").strip() or "_Sin descripción_"
            mat   = m.get("config", {}).get("materialized", "?")
            alias = m.get("alias") or name
            tags  = ", ".join(m.get("tags", [])) or "—"
            path  = m.get("original_file_path", "")

            header_row = [
                Paragraph(f"<b>{name}</b>", H3),
                Paragraph(f"<font color='#666666'>Materialización:</font> <b>{mat}</b> &nbsp;|&nbsp; "
                          f"<font color='#666666'>Alias:</font> {alias} &nbsp;|&nbsp; "
                          f"<font color='#666666'>Tags:</font> {tags}", SMALL),
            ]
            story.append(Table([header_row], colWidths=[7*cm, 9*cm], style=TableStyle([
                ("VALIGN", (0,0),(-1,-1), "MIDDLE"),
                ("LEFTPADDING",(0,0),(-1,-1),0),
                ("BOTTOMPADDING",(0,0),(-1,-1),1),
            ])))
            story.append(Paragraph(f"<i>{path}</i>", SMALL))
            story.append(Paragraph(desc, BODY))

            # Columns table
            cols = m.get("columns", {})
            if cols:
                col_data = [["Columna", "Descripción"]]
                for cname, cval in cols.items():
                    cdesc = cval.get("description", "").strip() or "—"
                    col_data.append([
                        Paragraph(f"<b>{cname}</b>", SMALL),
                        Paragraph(cdesc, SMALL)
                    ])
                col_tbl = Table(col_data, colWidths=[5.5*cm, 10.5*cm], style=tbl_style)
                story.append(col_tbl)

            story.append(Spacer(1, 0.25*cm))
            story.append(HRFlowable(width="100%", thickness=0.3, color=colors.HexColor("#dddddd")))
            story.append(Spacer(1, 0.1*cm))

        story.append(PageBreak())

    # ── SCD / Version Control section ────────────────────────────────────────
    story.append(Paragraph("Control de Versiones — SCD Type 2 y Snapshots Incrementales", H1))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#8172B2")))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph(
        "Este proyecto no usa dbt <b>snapshots</b> nativos. En su lugar implementa patrones de "
        "<b>SCD (Slowly Changing Dimension) Type 2</b> y <b>snapshots diarios incrementales</b> "
        "directamente en modelos incrementales con lógica T-SQL explícita.",
        BODY
    ))
    story.append(Spacer(1, 0.2*cm))

    scd_items = [
        {
            "name": "fct_operator_scd",
            "tipo": "SCD Type 2 — Operadores",
            "grain": "(operator_id, valid_from)",
            "mecanismo": "Pre-hook UPDATE is_current=0 en registros activos → INSERT nuevas versiones con is_current=1",
            "campos_clave": "scd_id (PK), operator_id, valid_from, valid_to, is_current",
        },
        {
            "name": "fct_route_scd",
            "tipo": "SCD Type 2 — Rutas",
            "grain": "(operator_id, route_id, valid_from)",
            "mecanismo": "Mismo patrón que fct_operator_scd: UPDATE + INSERT via pre_hook",
            "campos_clave": "scd_id (PK), route_id, operator_id, valid_from, is_current",
        },
        {
            "name": "fct_sonnell_subsystem_cost",
            "tipo": "SCD Type 2 — Costo Subsistema Sonnell",
            "grain": "DailySummaryId",
            "mecanismo": "Pre-hook marca CurrentVersion=0 en versiones anteriores. INSERT con NOT EXISTS guard. Reprocess: DELETE + INSERT para el mes indicado.",
            "campos_clave": "DailySummaryId (PK), CurrentVersion (BIT), ServiceDate, Version",
        },
        {
            "name": "fct_sonnell_subsystem_offset",
            "tipo": "SCD Type 2 — Offset Subsistema Sonnell",
            "grain": "(Year, Month, Subsystem, Version)",
            "mecanismo": "Mismo patrón SCD que fct_sonnell_subsystem_cost",
            "campos_clave": "Year, Month, Subsystem, Version, CurrentVersion (BIT)",
        },
        {
            "name": "fct_sonnell_invoice_totals",
            "tipo": "SCD Type 2 — Totales de Factura Sonnell",
            "grain": "(Year, Month, Subsystem, Type, Version)",
            "mecanismo": "Pre-hook SCD + 10 post-hooks secuenciales: InvoiceID, SonnellOffsetApplied, propagación a tablas relacionadas, Trips, RevenueMiles, Checkpoints, Timestamps",
            "campos_clave": "Year, Month, Subsystem, Type, Version, CurrentVersion (BIT), InvoiceID",
        },
        {
            "name": "AMA_Geotab_GPS_Comunicacion",
            "tipo": "Snapshot Diario Incremental",
            "grain": "(device_id, snapshot_date)",
            "mecanismo": "Pre-hook DELETE del snapshot de hoy + re-INSERT. Idempotente. Soporta backfill con var gps_backfill_date.",
            "campos_clave": "device_id, snapshot_date, categoria_comunicacion, categoria_gps",
        },
    ]

    scd_table_data = [["Modelo", "Tipo", "Grain", "Mecanismo", "Campos Clave"]]
    for item in scd_items:
        scd_table_data.append([
            Paragraph(f"<b>{item['name']}</b>", SMALL),
            Paragraph(item["tipo"], SMALL),
            Paragraph(f"<i>{item['grain']}</i>", SMALL),
            Paragraph(item["mecanismo"], SMALL),
            Paragraph(item["campos_clave"], SMALL),
        ])

    scd_tbl = Table(
        scd_table_data,
        colWidths=[4*cm, 3.5*cm, 3.5*cm, 5*cm, 0.1*cm],
        style=tbl_style
    )
    # Adjust widths
    scd_tbl = Table(
        scd_table_data,
        colWidths=[3.8*cm, 3*cm, 3*cm, 4.5*cm, 3*cm],
        style=tbl_style
    )
    story.append(scd_tbl)

    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("Modos de ejecución (Sonnell fact models)", H2))
    exec_data = [
        ["Modo", "Comando", "Comportamiento"],
        ["Full Refresh",       "dbt run --full-refresh -s <model>",
         "Reconstruye desde cero. Sin pre_hook de SCD."],
        ["Incremental Diario", "dbt run -s <model>",
         "Pre_hook marca CurrentVersion=0. INSERT con NOT EXISTS guard. Solo procesa meses cerrados."],
        ["Reprocess Mensual",  "dbt run -s <model> --vars '{reprocess: true, month: M, year: Y}'",
         "Pre_hook DELETE del mes indicado. INSERT fresh para ese mes."],
    ]
    exec_tbl = Table(exec_data, colWidths=[3.5*cm, 6.5*cm, 7*cm], style=tbl_style)
    story.append(exec_tbl)

    doc.build(story, onFirstPage=header, onLaterPages=header)
    print(f"  ✓ {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# PDF 3 — TESTS POR MODELO
# ═══════════════════════════════════════════════════════════════════════════════
def pdf_tests():
    out_path = os.path.join(OUTPUT_DIR, "03_tests_por_modelo.pdf")
    doc = SimpleDocTemplate(
        out_path, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2.2*cm, bottomMargin=1.8*cm
    )
    def header(c, d): page_header(c, d, "Tests por Modelo")

    story = []
    story.append(Paragraph("Tests de Calidad de Datos — por Modelo", TITLE))
    story.append(Paragraph(
        f"<b>{len(tests_raw)}</b> tests definidos sobre <b>{len(models)}</b> modelos · "
        f"Generado: {GENERATED_AT}",
        BODY
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#1a2f5e")))
    story.append(Spacer(1, 0.3*cm))

    tbl_style = TableStyle([
        ("BACKGROUND", (0,0), (-1,0), TABLE_HEADER),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE",   (0,0), (-1,-1), 8),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [TABLE_ROW_A, TABLE_ROW_B]),
        ("GRID",       (0,0), (-1,-1), 0.4, colors.HexColor("#cccccc")),
        ("VALIGN",     (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0),(-1,-1), 5),
        ("RIGHTPADDING",(0,0),(-1,-1), 5),
        ("TOPPADDING",  (0,0),(-1,-1), 3),
        ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ])

    # Group tests by attached model
    tests_by_model = defaultdict(list)
    for uid, t in tests_raw.items():
        attached = t.get("attached_node", "")
        meta = t.get("test_metadata", {})
        tests_by_model[attached].append({
            "test_type": meta.get("name", t.get("name","?")),
            "column":    t.get("column_name", "—"),
            "kwargs":    meta.get("kwargs", {}),
            "uid":       uid,
        })

    # Summary chart
    test_type_counts = defaultdict(int)
    for t_list in tests_by_model.values():
        for t in t_list:
            test_type_counts[t["test_type"]] += 1

    fig, ax = plt.subplots(figsize=(8, 3))
    fig.patch.set_facecolor("#f8f9fc")
    labels = list(test_type_counts.keys())
    values = [test_type_counts[l] for l in labels]
    bar_colors = ["#4C72B0","#55A868","#C44E52","#8172B2","#CCB974","#64B5CD","#AAAAAA"]
    ax.barh(labels, values, color=bar_colors[:len(labels)], alpha=0.85)
    for i, v in enumerate(values):
        ax.text(v + 0.3, i, str(v), va="center", fontsize=9)
    ax.set_xlabel("Cantidad de tests")
    ax.set_title("Distribución de tipos de test", fontsize=11, color="#1a2f5e")
    ax.set_facecolor("#f8f9fc")
    plt.tight_layout()
    chart_path = os.path.join(OUTPUT_DIR, "_test_chart.png")
    plt.savefig(chart_path, dpi=120, bbox_inches="tight", facecolor="#f8f9fc")
    plt.close()
    story.append(Image(chart_path, width=14*cm, height=5*cm))
    story.append(Spacer(1, 0.3*cm))

    # Tests by domain
    domains_order = ["sonnell", "ama", "hms", "monitoring", "ati_datawarehouse", "other"]
    domain_labels = {
        "sonnell":          "Sonnell",
        "ama":              "AMA / Geotab",
        "hms":              "HMS",
        "monitoring":       "Monitoring",
        "ati_datawarehouse":"ATI Datawarehouse",
        "other":            "Legacy / Ad-hoc",
    }

    by_domain = defaultdict(list)
    for uid, m in models.items():
        by_domain[get_domain(m)].append((uid, m))

    for domain in domains_order:
        domain_models = sorted(by_domain.get(domain, []), key=lambda x: x[1]["name"])
        if not domain_models:
            continue

        domain_tests = sum(len(tests_by_model.get(uid, [])) for uid, _ in domain_models)
        if domain_tests == 0:
            story.append(Paragraph(f"{domain_labels.get(domain)} — <i>Sin tests definidos</i>", H2))
            story.append(Spacer(1, 0.1*cm))
            continue

        story.append(Paragraph(f"{domain_labels.get(domain)} ({domain_tests} tests)", H1))
        story.append(HRFlowable(width="100%", thickness=0.5,
                                 color=colors.HexColor(DOMAIN_COLORS.get(domain,"#aaa"))))
        story.append(Spacer(1, 0.1*cm))

        for uid, m in domain_models:
            model_tests = tests_by_model.get(uid, [])
            if not model_tests:
                continue

            story.append(Paragraph(f"<b>{m['name']}</b>  ({len(model_tests)} tests)", H3))

            test_data = [["Tipo de Test", "Columna", "Parámetros adicionales"]]
            for t in sorted(model_tests, key=lambda x: (x["column"] or "", x["test_type"] or "")):
                kwargs_str = ", ".join(
                    f"{k}={v}" for k, v in t["kwargs"].items()
                    if k not in ("model", "column_name") and not str(v).startswith("{{")
                ) or "—"
                test_data.append([
                    Paragraph(f"<b>{t['test_type']}</b>", SMALL),
                    Paragraph(t["column"] or "—", SMALL),
                    Paragraph(kwargs_str[:120], SMALL),
                ])

            tbl = Table(test_data, colWidths=[4*cm, 4*cm, 9*cm], style=tbl_style)
            story.append(tbl)
            story.append(Spacer(1, 0.2*cm))

        story.append(PageBreak())

    doc.build(story, onFirstPage=header, onLaterPages=header)
    print(f"  ✓ {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# PDF 4 — FUENTES REGISTRADAS
# ═══════════════════════════════════════════════════════════════════════════════
def pdf_sources():
    out_path = os.path.join(OUTPUT_DIR, "04_fuentes_registradas.pdf")
    doc = SimpleDocTemplate(
        out_path, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2.2*cm, bottomMargin=1.8*cm
    )
    def header(c, d): page_header(c, d, "Fuentes Registradas")

    story = []
    story.append(Paragraph("Fuentes de Datos Registradas", TITLE))
    story.append(Paragraph(
        f"<b>{len(all_sources)}</b> tablas fuente en <b>{len(set(s['source_name'] for s in all_sources.values()))}</b> "
        f"sources · Generado: {GENERATED_AT}",
        BODY
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#1a2f5e")))
    story.append(Spacer(1, 0.3*cm))

    tbl_style = TableStyle([
        ("BACKGROUND", (0,0), (-1,0), TABLE_HEADER),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE",   (0,0), (-1,-1), 8),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [TABLE_ROW_A, TABLE_ROW_B]),
        ("GRID",       (0,0), (-1,-1), 0.4, colors.HexColor("#cccccc")),
        ("VALIGN",     (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0),(-1,-1), 5),
        ("RIGHTPADDING",(0,0),(-1,-1), 5),
        ("TOPPADDING",  (0,0),(-1,-1), 3),
        ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ])

    # Group by source_name
    by_source = defaultdict(list)
    for uid, s in all_sources.items():
        by_source[s["source_name"]].append(s)

    # Summary pie chart
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#f8f9fc")
    src_names = list(by_source.keys())
    src_counts = [len(by_source[s]) for s in src_names]
    pie_colors = ["#4C72B0","#55A868","#C44E52","#8172B2","#CCB974","#64B5CD",
                  "#AAAAAA","#E377C2","#7F7F7F","#BCBD22","#17BECF"]
    wedges, texts, autotexts = ax.pie(
        src_counts, labels=src_names, autopct="%1.0f%%",
        colors=pie_colors[:len(src_names)], startangle=90,
        textprops={"fontsize": 7}
    )
    ax.set_title("Tablas por Source", fontsize=11, color="#1a2f5e")
    plt.tight_layout()
    pie_path = os.path.join(OUTPUT_DIR, "_source_pie.png")
    plt.savefig(pie_path, dpi=120, bbox_inches="tight", facecolor="#f8f9fc")
    plt.close()
    story.append(Image(pie_path, width=12*cm, height=7*cm))
    story.append(Spacer(1, 0.3*cm))

    # Source catalog
    for sname in sorted(by_source.keys()):
        tables = sorted(by_source[sname], key=lambda s: s["name"])
        first  = tables[0]
        schema = first.get("schema", "?")
        db     = first.get("database", "?")
        src_desc = first.get("source_description", "").strip()

        story.append(Paragraph(f"Source: <b>{sname}</b>", H1))
        story.append(Paragraph(
            f"<font color='#666666'>Database:</font> {db} &nbsp;·&nbsp; "
            f"<font color='#666666'>Schema:</font> {schema} &nbsp;·&nbsp; "
            f"<font color='#666666'>Tablas:</font> {len(tables)}",
            SMALL
        ))
        if src_desc:
            story.append(Paragraph(src_desc, BODY))
        story.append(Spacer(1, 0.15*cm))

        # Tables summary
        tbl_data = [["Tabla (identifier)", "Alias dbt", "Descripción", "Columnas doc."]]
        for s in tables:
            n_cols = len(s.get("columns", {}))
            tbl_data.append([
                Paragraph(f"<b>{s.get('identifier', s['name'])}</b>", SMALL),
                Paragraph(s["name"], SMALL),
                Paragraph((s.get("description","") or "—")[:120], SMALL),
                Paragraph(str(n_cols) if n_cols else "—", SMALL),
            ])

        story.append(Table(tbl_data, colWidths=[4.5*cm, 4*cm, 6.5*cm, 2*cm], style=tbl_style))
        story.append(Spacer(1, 0.2*cm))

        # Detail per table if has columns documented
        for s in tables:
            cols = s.get("columns", {})
            if not cols:
                continue
            story.append(Paragraph(f"  Columnas documentadas: <b>{s['name']}</b>", H3))
            col_data = [["Columna", "Descripción"]]
            for cname, cval in cols.items():
                col_data.append([
                    Paragraph(f"<b>{cname}</b>", SMALL),
                    Paragraph((cval.get("description","") or "—"), SMALL),
                ])
            col_tbl = Table(col_data, colWidths=[5*cm, 12*cm], style=tbl_style)
            story.append(col_tbl)
            story.append(Spacer(1, 0.15*cm))

        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc")))
        story.append(Spacer(1, 0.2*cm))

    doc.build(story, onFirstPage=header, onLaterPages=header)
    print(f"  ✓ {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"\nGenerando PDFs en '{OUTPUT_DIR}/'...\n")
    print("PDF 1 — Linaje DAG...")
    pdf_lineage()
    print("PDF 2 — Descripciones de modelos...")
    pdf_model_descriptions()
    print("PDF 3 — Tests por modelo...")
    pdf_tests()
    print("PDF 4 — Fuentes registradas...")
    pdf_sources()
    print(f"\n✅  4 PDFs generados en '{OUTPUT_DIR}/'")
    # Cleanup temp images
    for f in os.listdir(OUTPUT_DIR):
        if f.startswith("_") and f.endswith(".png"):
            os.remove(os.path.join(OUTPUT_DIR, f))
    print("   Imágenes temporales eliminadas.")
