Common errors and resolutions:

* **Issue:** Map is rendering but no background 
* **Cause:** Check st.pydeck_chart function in the Streamlit Application 
* **Solution:** Make sure the code matches below:
```python
    st.pydeck_chart(
        pdk.Deck(
            map_style="light",# This gives you a light theme
            initial_view_state=view_state,
            tooltip={"html": "<b>Value:</b> {COUNT}", "style": {"color": "white"}},
            layers=[h3_layer],
        )
    )
```
