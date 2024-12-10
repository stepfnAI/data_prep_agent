from sfn_blueprint.views.streamlit_view import SFNStreamlitView
from typing import List, Optional
import streamlit as st
from typing import Any

class StreamlitView(SFNStreamlitView):
    @property
    def session_state(self):
        """Access to Streamlit's session state"""
        return st.session_state
    
    def select_box(self, label: str, options: List[str], key: Optional[str] = None, default: Optional[str] = None) -> str:
        # Find the index of the default value if provided
        if default and default in options:
            index = options.index(default)
        else:
            index = 0
        return st.session_state(label, options, key=key, index=index)
    
    def file_uploader(self, label: str, key: str, accepted_types: List[str]) -> Optional[str]:
        return st.file_uploader(label, key=key, type=accepted_types)
    
    
    def checkbox(self, label=None, key=None, value=False, disabled=False, label_visibility="visible"):
        """Create a checkbox with a default hidden label if none provided"""
        if label is None or label == "":
            # Generate a label based on the key if no label is provided
            label = key if key else "checkbox"
        return st.checkbox(
            label=label,
            key=key,
            value=value,
            disabled=disabled,
            label_visibility=label_visibility
        )
    
    def display_button(self, label: str, key: Optional[str] = None) -> bool:
        """Display a button with proper labeling"""
        button_key = key if key else f"button_{label}"
        return st.button(label=label, key=button_key)
