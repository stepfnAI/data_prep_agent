from sfn_blueprint.views.streamlit_view import SFNStreamlitView
from typing import List, Optional
import streamlit as st

class StreamlitView(SFNStreamlitView):
    
    def select_box(self, label: str, options: List[str], key: Optional[str] = None) -> str:
        return st.selectbox(label, options, key=key)

    