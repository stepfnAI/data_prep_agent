from sfn_blueprint.views.streamlit_view import SFNStreamlitView
from typing import List, Optional
import streamlit as st
from typing import Any

class StreamlitView(SFNStreamlitView):
    @property
    def session_state(self):
        """Access to Streamlit's session state"""
        return st.session_state
    
    def select_box(self, label: str, options: List[str], key: Optional[str] = None) -> str:
        return st.selectbox(label, options, key=key)
    
    def file_uploader(self, label: str, key: str, accepted_types: List[str]) -> Optional[str]:
        return st.file_uploader(label, key=key, type=accepted_types)
