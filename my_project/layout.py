import dash_bootstrap_components as dbc
from dash import dcc, html

def banner():
    """Build the banner at the top of the page."""
    return html.Div(
        id="banner",
        children=[
            dbc.Row(
                align="center",
                children=[
                    dbc.Col(
                        html.A(
                            href="/",
                            children=[
                                html.Img(
                                    src="assets/img/cbe-logo-small.png",
                                ),
                            ],
                        ),
                        width="auto",
                    ),
                    dbc.Col(
                        children=[
                            html.H1(id="banner-title", children=["PROGETTO: Modelli e tecniche per i Big Data"]),
                            html.H5(
                                id="banner-subtitle",
                                children=["Stazione selezionata:"],
                            ),
                        ],
                    ),
                    dbc.Col(
                        style={"fontWeight": "400", "padding": "1rem"},
                        align="end",
                        children=[
                            dbc.Row(
                                style={"text-align": "right"},
                                children=[
                                    dbc.RadioItems(
                                        options=[
                                            #{
                                            #    "label": "Global Value Ranges",
                                            #    "value": "global",
                                            #},
                                            {
                                                "label": "Local Value Ranges",
                                                "value": "local",
                                            },
                                        ],
                                        value="local",
                                        id="global-local-radio-input",
                                        inline=True,
                                    ),
                                ],
                            ),
                            dbc.Row(
                                align="end",
                                style={"text-align": "right"},
                                children=[
                                    dbc.RadioItems(
                                        options=[
                                            {"label": "SI", "value": "si"},
                                            #{"label": "IP", "value": "ip"},
                                        ],
                                        value="si",
                                        id="si-ip-radio-input",
                                        inline=True,
                                    ),
                                ],
                            ),
                        ],
                        width="auto",
                    ),
                ],
            ),
        ],
    )


def build_tabs():
    """Build the seven different tabs."""
    return html.Div(
        id="tabs-container",
        children=[
            dcc.Tabs(
                id="tabs",
                parent_className="custom-tabs",
                value="tab-select",
                children=[
                    dcc.Tab(
                        label="Select Weather File",
                        value="tab-select",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                    ),
                    dcc.Tab(
                        id="tab-summary",
                        label="Climate Summary",
                        value="tab-summary",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                    dcc.Tab(
                        id="tab-t-rh",
                        label="Temperature and Humidity",
                        value="tab-t-rh",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                    dcc.Tab(
                        id="tab-sun",
                        label="Sun Path",
                        value="tab-sun",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                    dcc.Tab(
                        id="tab-wind",
                        label="Wind",
                        value="tab-wind",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                    dcc.Tab(
                        id="tab-psy-chart",
                        label="Psychrometric Chart",
                        value="tab-psy-chart",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                    dcc.Tab(
                        id="tab-natural-ventilation",
                        label="Natural Ventilation",
                        value="tab-natural-ventilation",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                    dcc.Tab(
                        id="tab-outdoor-comfort",
                        label="Outdoor Comfort",
                        value="tab-outdoor-comfort",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                    dcc.Tab(
                        id="tab-data-explorer",
                        label="Data Explorer",
                        value="tab-data-explorer",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                        disabled=True,
                    ),
                ],
            ),
            html.Div(
                id="store-container",
                children=[store(), html.Div(id="tabs-content")],
            ),
        ],
    )


def store():
    return html.Div(
        id="store",
        children=[
            dcc.Loading(
                [
                    dcc.Store(id="df-store", storage_type="session"),
                    dcc.Store(id="meta-store", storage_type="session"),
                    dcc.Store(id="url-store", storage_type="session"),
                    dcc.Store(id="si-ip-unit-store", storage_type="session"),
                    dcc.Store(id="lines-store", storage_type="session"),
                    dcc.Store(id="df-cluster-store", storage_type="session"),
                    dcc.Store(id="location", storage_type="session"),
                ],
                fullscreen=True,
                type="dot",
            )
        ],
    )
