import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st

# ===============================
# CONFIG
# ===============================
st.set_page_config(page_title="One Piece — Dashboard", layout="wide")
GOLD_DIR = "datalake/gold"

# ===============================
# LOAD: CSV mais recente da GOLD
# ===============================
@st.cache_data
def load_latest_gold():
    if not os.path.exists(GOLD_DIR):
        return None, pd.DataFrame()
    csvs = [f for f in os.listdir(GOLD_DIR) if f.endswith(".csv")]
    if not csvs:
        return None, pd.DataFrame()
    latest_path = max([os.path.join(GOLD_DIR, f) for f in csvs], key=os.path.getctime)
    df = pd.read_csv(latest_path)
    return os.path.basename(latest_path), df

file_name, df_raw = load_latest_gold()

# ===============================
# HELPERS de normalização
# ===============================
STATUS_PRIORITY = {"Desconhecido": 0, "Morto": 1, "Vivo": 2}

def norm_status(x: str) -> str:
    if pd.isna(x):
        return "Desconhecido"
    s = str(x).strip().lower()
    if s in {"alive", "vivant", "living", "vivo"}:
        return "Vivo"
    if s in {"dead", "deceased", "morto"}:
        return "Morto"
    if s in {"unknown", "desconhecido", "unk"}:
        return "Desconhecido"
    return s.capitalize() if s else "Desconhecido"

def has_fruit_func(x) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s not in {"", "não tem", "nao tem", "none", "null", "sem fruta", "no", "nan"}

def first_non_empty(series: pd.Series):
    for v in series:
        if pd.notna(v) and str(v).strip():
            return v
    return None

def mode_or_first(series: pd.Series, default=""):
    s = series.dropna().astype(str)
    s = s[s.str.strip() != ""]
    if s.empty:
        return default
    m = s.mode()
    return (m.iloc[0] if not m.empty else s.iloc[0])

def status_best(series: pd.Series):
    s = series.apply(norm_status)
    if s.empty:
        return "Desconhecido"
    return s.loc[s.map(STATUS_PRIORITY).idxmax()]

def emperor_any(series: pd.Series):
    vals = series.astype(str).str.strip().str.lower()
    return "Sim" if any(v in {"sim", "true", "1"} for v in vals) else "Não"

# ===============================
# PRÉ-PROCESSAMENTO + DEDUP
# ===============================
def preprocess_and_dedup(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    # strings básicas
    for col in ["name", "crew", "fruit", "status", "job", "emperor", "image_url", "fruit_image_url"]:
        if col in out.columns:
            out[col] = out[col].astype(str).replace({"nan": ""}).fillna("")

    # bounty numérico
    if "bounty" in out.columns:
        out["bounty_num"] = pd.to_numeric(out["bounty"], errors="coerce").fillna(0)
    else:
        out["bounty_num"] = 0

    # normalizados para filtro
    out["name"] = out["name"].astype(str).str.strip() if "name" in out.columns else pd.Series([""] * len(out))
    out["crew_norm"] = (
        out["crew"].astype(str).str.strip().replace({"": "Sem tripulação"})
        if "crew" in out.columns else pd.Series(["Sem tripulação"] * len(out))
    )
    out["fruit_norm"] = out["fruit"].astype(str).str.strip() if "fruit" in out.columns else pd.Series(["Não tem"] * len(out))
    out["status_norm"] = out["status"].apply(norm_status) if "status" in out.columns else pd.Series(["Desconhecido"] * len(out))
    out["has_fruit"] = out["fruit_norm"].apply(has_fruit_func)

    # chave de dedup
    key_col = "id" if "id" in out.columns else None
    if key_col is None:
        out["name_key"] = out["name"].str.casefold()
        key_col = "name_key"

    out = out.sort_values(["bounty_num"], ascending=False)

    # agg_dict dinâmico
    agg_dict = {
        "name": "first",
        "bounty_num": "max",
        "crew_norm": mode_or_first,
        "fruit_norm": mode_or_first,
        "status_norm": status_best,
        "job": mode_or_first if "job" in out.columns else "first",
        "emperor": emperor_any if "emperor" in out.columns else "first",
        "has_fruit": "max",
    }

    if "id" in out.columns:
        agg_dict["id"] = "first"
    if "image_url" in out.columns:
        agg_dict["image_url"] = first_non_empty
    if "fruit_image_url" in out.columns:
        agg_dict["fruit_image_url"] = first_non_empty

    deduped = out.groupby(key_col, as_index=False).agg(agg_dict)
    deduped = deduped.sort_values("bounty_num", ascending=False)

    return deduped

df = preprocess_and_dedup(df_raw)

# ===============================
# UI / SIDEBAR
# ===============================
st.sidebar.info(f"📂 Usando: {file_name if file_name else '— sem arquivo —'}")
st.sidebar.success(f"🧹 Personagens únicos: {len(df)}")

st.title("🏴‍☠️ One Piece — Dashboard")
st.markdown("Explore os personagens com recompensas, status e frutas. Os gráficos e a lista respeitam os filtros.")

# Filtros
st.sidebar.header("🔎 Filtros")
search_name = st.sidebar.text_input("Buscar por nome:", value="").strip()

crew_vals = sorted(df["crew_norm"].dropna().unique().tolist()) if not df.empty else []
crew_choice = st.sidebar.selectbox("Tripulação:", ["Todos"] + crew_vals)

status_vals = ["Vivo", "Morto", "Desconhecido"]
status_choice = st.sidebar.selectbox("Status:", ["Todos"] + status_vals)

fruit_choice = st.sidebar.radio("Possui fruta?", ["Todos", "Sim", "Não"], index=0)

# aplica filtros
df_f = df.copy()

if search_name:
    sn = search_name.lower()
    df_f = df_f[df_f["name"].str.lower().str.contains(sn, na=False)]

if crew_choice != "Todos":
    df_f = df_f[df_f["crew_norm"] == crew_choice]

if status_choice != "Todos":
    df_f = df_f[df_f["status_norm"] == status_choice]

if fruit_choice == "Sim":
    df_f = df_f[df_f["has_fruit"]]
elif fruit_choice == "Não":
    df_f = df_f[~df_f["has_fruit"]]

# ===============================
# GRÁFICOS
# ===============================
st.subheader("📊 Análises Automáticas")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**Top 10 Maiores Recompensas (por personagem)**")
    if not df_f.empty:
        top10 = df_f.head(10)
        fig, ax = plt.subplots()
        ax.barh(top10["name"], top10["bounty_num"] / 1_000_000)
        ax.set_xlabel("Recompensa (em milhões)")
        ax.invert_yaxis()
        fig.tight_layout()
        st.pyplot(fig)
    else:
        st.info("Sem dados disponíveis.")

with col2:
    st.markdown("**Distribuição de Status**")
    if not df_f.empty:
        status_count = df_f["status_norm"].value_counts().reindex(status_vals, fill_value=0)
        fig, ax = plt.subplots()
        ax.bar(status_count.index, status_count.values)
        ax.set_ylabel("Quantidade")
        for i, v in enumerate(status_count.values):
            ax.text(i, v, str(v), ha="center", va="bottom")
        fig.tight_layout()
        st.pyplot(fig)
    else:
        st.info("Sem dados disponíveis.")

with col3:
    st.markdown("**Fruta vs. Não Fruta**")
    if not df_f.empty:
        fruit_count = df_f["has_fruit"].map({True: "Com Fruta", False: "Sem Fruta"}).value_counts()
        fruit_count = fruit_count.reindex(["Sem Fruta", "Com Fruta"], fill_value=0)
        fig, ax = plt.subplots()
        ax.bar(fruit_count.index, fruit_count.values)
        ax.set_ylabel("Quantidade")
        for i, v in enumerate(fruit_count.values):
            ax.text(i, v, str(v), ha="center", va="bottom")
        fig.tight_layout()
        st.pyplot(fig)
    else:
        st.info("Sem dados disponíveis.")

# ===============================
# LISTAGEM (com imagem da fruta)
# ===============================
st.subheader(f"📋 Personagens ({len(df_f)})")

if df_f.empty:
    st.warning("Nenhum personagem encontrado com os filtros selecionados.")
else:
    qtd = st.sidebar.slider("Qtd. de cards para mostrar", 10, 200, 30, step=10)
    view = df_f.head(qtd)

    for _, info in view.iterrows():
        st.markdown("---")

        # layout: imagem da fruta à esquerda, infos à direita
        cols = st.columns([1, 3])

        with cols[0]:
            fruit_img = info.get("fruit_image_url", "")
            if isinstance(fruit_img, str) and fruit_img.strip() and fruit_img.lower() not in {"nan", "não disponível"}:
                st.image(fruit_img, width=120, caption=info.get("fruit_norm", "Fruta"))
            else:
                st.write("🍏 Sem fruta")

        with cols[1]:
            st.markdown(f"### {info.get('name', 'Desconhecido')}")
            st.write(f"**Tripulação:** {info.get('crew_norm', 'Sem tripulação')}")
            st.write(f"**Recompensa:** {int(info.get('bounty_num', 0)):,} Berries".replace(",", "."))
            st.write(f"**Fruta:** {info.get('fruit_norm', 'Não tem') or 'Não tem'}")
            st.write(f"**Status:** {info.get('status_norm', 'Desconhecido')}")
            st.write(f"**Cargo:** {info.get('job', 'Outro')}")
            st.write(f"**Yonkō:** {info.get('emperor', 'Não')}")
