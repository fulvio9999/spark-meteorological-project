import base64
import io
import json
import re
import dash
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import pandas as pd

from app import app
from jobs.spark_jobs import clustering
from my_project.extract_df import create_df, get_data, get_location_info
from my_project.utils import plot_location_epw_files, generate_chart_name
from my_project.global_scheme import mapping_dictionary
from my_project.extract_df import convert_data

from dash_extensions.enrich import ServersideOutput, Output, Input, State, html, dcc


def layout_select(clustered_data, location):
    """Contents in the first tab 'Select Weather File'"""
    return html.Div(
        className="container-col tab-container",
        children=[
            dbc.Row(
                [
                # align="end",
                # style={"text-align": "right"},
                dbc.Col(
                    [
                        dbc.Checklist(
                        options=[
                            {"label": "Clustering", "value": 1},
                            # {"label": "Option 2", "value": 2},
                            # {"label": "Disabled Option", "value": 3, "disabled": True},
                        ],
                        value=[] if clustered_data is None else [1],
                        id="clustering-switch",
                        switch=True,
                    ),
                        dbc.Checklist(
                            options=[
                                {"label": "Location Info", "value": 1},
                            ],
                            value=[] if location is None else [1],
                            id="location-switch",
                            switch=True,
                        ),
                        # dbc.Label("Numero di cluster:"),
                        # dbc.Input(
                        #     id="cluster-number",
                        #     type="number",
                        #     value=5,  # Valore iniziale
                        # ),
                    ], width=2
                ),
                dbc.Col(
                    [
                        dbc.Label("Numero di cluster:"),
                    ], width=1
                ),
                dbc.Col(
                    [
                        # dbc.Label("Numero di cluster:"),
                        dbc.Input(
                            id="cluster-number",
                            type="number",
                            value=5,  # Valore iniziale
                        ),
                    ], width=1
                ),
                ],
            ),
            dcc.Loading(
                id="loading-1",
                type="circle",
                fullscreen=True,
            ),
            dcc.Graph(
                id="tab-one-map",
                figure=plot_location_epw_files(),
                config=generate_chart_name("epw_location_select"),
            ),
            dbc.Modal(
                [
                    dbc.ModalHeader(id="modal-header"),
                    dbc.ModalFooter(
                        children=[
                            dbc.Button(
                                "Close",
                                id="modal-close-button",
                                className="ml-2",
                                color="light",
                            ),
                            dbc.Button(
                                "Yes",
                                id="modal-yes-button",
                                className="ml-2",
                                color="primary",
                            ),
                        ]
                    ),
                ],
                id="modal",
                is_open=False,
            ),
            html.Div(id='output-text'),
        ],
    )
    
 # add si-ip and map dictionary in the output
@app.callback(
    [
        # Output('tab-one-map', 'figure'),
        ServersideOutput("df-cluster-store", "data"),
    ],
    [
        Input("clustering-switch", "value"),
    ],
    [
        State("cluster-number", "value"),
        State("df-cluster-store", "data"),
        State("location", "value")
    ],
    # prevent_initial_call=True,
)
# @code_timer
def clustering_on(
    clustering_switch,
    k,
    clustered_data,
    location
):
    print("La location in cluster è: ", location)
    if clustering_switch != [] and clustering_switch[0] == 1 and clustered_data is None:
        print("CLUSTER ON")
        print(k)
        if k==5 and location is not None and location:
            clustered_data = pd.read_csv('./assets/data/clustered_data_5_location.csv')
        elif k==5:
            clustered_data = pd.read_csv('./assets/data/clustered_data_5.csv')
        else:
            clustered_data = clustering(k, False if location is None else location)
        # clustered_data.to_csv('./clustered_data_5_location.csv', index=False)
        return clustered_data
    elif clustering_switch == []:
        return None
    return clustered_data

@app.callback(
    [
        # Output('tab-one-map', 'figure'),
        ServersideOutput("location", "value"),
    ],
    [
        Input("location-switch", "value"),
    ],
    [
        State("location", "value")
    ],
    # prevent_initial_call=True,
)
# @code_timer
def location_on(
    location_switch,
    location,
):
    if location_switch != [] and location_switch[0] == 1 and location is None:
        return True
    elif location_switch == []:
        return None
    return location
    
# add si-ip and map dictionary in the output
@app.callback(
    [
        Output('tab-one-map', 'figure'),
        # Output("df-cluster-store", "data"),
    ],
    [
        Input("df-cluster-store", "data")
        # Input("clustering-switch", "value"),
    ],
    # [
    #     State("cluster-number", "value"),
    # ],
    # prevent_initial_call=True,
)
# @code_timer
def clustering_on(
    clustered_data
    # clustering_switch,
    # k,
):
    return plot_location_epw_files(clustered_data)
    # if clustering_switch != [] and clustering_switch[0] == 1:
    #     print("CLUSTER ON")
    #     print(k)
    #     clustered_data = clustering(k)
    #     # clustered_data = pd.read_csv('./assets/data/clustering.csv')
    #     return plot_location_epw_files(clustered_data)#, clustered_data
    # return plot_location_epw_files()#, None

# add si-ip and map dictionary in the output
@app.callback(
    [
        ServersideOutput("df-store", "data"),
        Output("meta-store", "data"),
        Output("si-ip-unit-store", "data"),
    ],
    [
        Input("modal-yes-button", "n_clicks"),
    ],
    [
        State("url-store", "data"),
        State("df-cluster-store", "data"),
        State("location", "value"),
    ],
    prevent_initial_call=True,
)
# @code_timer
def submitted_data(
    use_epw_click,
    wban,
    clustered_data,
    location
):
    if use_epw_click is not None:
        if clustered_data is not None:
            wbans = clustered_data[clustered_data['prediction']==wban][['WBAN']].values.tolist()
            wbans = [int(wban[0]) for wban in wbans]
        else:
            wbans = [wban]
        # if wbans == [3171]:
        #     df = pd.read_csv('./assets/data/riverside_3171.csv')
        # elif 3171 in wbans and clustered_data is not None and location is not None and location:
        #     df = pd.read_csv('./assets/data/riverside_3171_cluster5_location.csv')
        # elif 3171 in wbans and clustered_data is not None:
        #     df = pd.read_csv('./assets/data/riverside_3171_cluster5.csv')
        # else:
        #     df = get_data(wbans)
        df = get_data(wbans)
            # df.to_csv('./assets/data/riverside_3171_cluster5_location.csv', index=False)
        location_info = get_location_info(wbans)
        si_ip = 'si' 
        return (df, location_info, si_ip)
    # else: return None
    raise PreventUpdate

@app.callback(
    [
        Output("tab-summary", "disabled"),
        Output("tab-t-rh", "disabled"),
        Output("tab-sun", "disabled"),
        Output("tab-wind", "disabled"),
        Output("tab-psy-chart", "disabled"),
        Output("tab-data-explorer", "disabled"),
        Output("tab-outdoor-comfort", "disabled"),
        Output("tab-natural-ventilation", "disabled"),
        Output("banner-subtitle", "children"),
    ],
    [
        Input("meta-store", "data"),
        Input("df-store", "data"),
    ],
)
def enable_tabs_when_data_is_loaded(meta, data):
    """Hide tabs when data are not loaded"""
    default = "Stazione selezionata: N/A"
    if data is None:
        return (
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            default,
        )
    else:
        cities = meta["city"][0]
        for i, c in enumerate(meta['city'][1:]):
            cities = cities + ", " + c
            if i>10:
                cities = cities + ", ..."
                break
        return (
            False,
            False,
            False if len(meta['city'])==1 else True,
            False,
            False,
            False,
            False,
            False,
            "Stazione selezionata: " + cities if len(meta["city"])==1 else "Stazioni selezionate: " + cities #+ ", " + meta["country"],
        )


@app.callback(
    [
        Output("modal", "is_open"),
        Output("url-store", "data"),
    ],
    [
        Input("modal-yes-button", "n_clicks"),
        Input("tab-one-map", "clickData"),
        Input("modal-close-button", "n_clicks"),
    ],
    [State("modal", "is_open")],
    prevent_initial_call=True,
)
def display_modal_when_data_clicked(clicks_use_epw, click_map, close_clicks, is_open):
    """display the modal to the user and check if he wants to use that file"""
    if click_map:
        try:
            wban = click_map["points"][0]["customdata"][0]
            # url = re.search(
            #     r'href=[\'"]?([^\'" >]+)', click_map["points"][0]["customdata"][0]
            # ).group(1)
        except:
            url = None
        return not is_open, wban #url
    return is_open, ""


@app.callback(
    [
        Output("modal-header", "children"),
    ],
    [
        Input("tab-one-map", "clickData"),
    ],
    State("df-cluster-store", "data"),
    prevent_initial_call=True,
)
def display_modal_when_data_clicked(click_map, clustered_data):
    """change the text of the modal header"""
    if click_map:
        return [f"Analyse data from {click_map['points'][0]['hovertext']}?"] if clustered_data is None else ["Analyse data from cluster selected?"]
    return ["Analyse data from this location?"]