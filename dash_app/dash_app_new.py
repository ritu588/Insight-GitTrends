import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from datetime import date, timedelta
import plotly.graph_objs as go
import pandas as pd
import os
import psycopg2
import json



#initial dash obj
app = dash.Dash()

#years in the database
activity = ['Aggregate','Push','Watch','Fork']
#initial the app layout
app.layout = html.Div([

    html.Div(
        ' ',
        style={'color':'blue', 'padding-right':10, 'width':'50%', 'display':'inline-block'}
        ),
        html.H3(
           children='Github Trends',
           style={
               'textAlign': 'center',
               'color': 'Black'
           }
        ),
        html.H5(
           children='20 Most popular repositories',
           style={
               'textAlign': 'center'
           }
        ),

        # html.Label(' Select an activity'),
        html.Div([
            dcc.Dropdown(
                id='activity',
                #options=[{'label': i, 'value': i} for i in activity],
                options = [
                    {'label': 'Aggregate Activity', 'value': 'aggregate'},
                    {'label': 'Push Events', 'value': 'push'},
                    {'label': 'Star Events', 'value': 'watch'},
                    {'label': 'Fork Events', 'value': 'fork'}
                ],
                value='aggregate'
            )
        ],
        style={'width': '30%', 'padding-left': '35%', 'padding-top':'5%', 'padding-bottom':'5%'}
        ),

    html.Div([
        dcc.Slider(
            id='slider_updatetime',
            min = 0,
            max = 100,
            step = None,
            marks={
                0: {'label': 'Last Week', 'style': {'color': '#77b0b1'}},
                10: {'label': 'Last Month'},
                50: {'label': 'Last Three Months'},
                100: {'label': 'Last Six Months', 'style': {'color': '#f50'}}
                }
            ),
    ],
    style={'width': '90%','padding-left':'5%', 'padding-right':'5%', 'padding-bottom':'5%'}),

    html.Div([
        dcc.Graph(
            id='barplot-topactivity',
            #hoverData={'points': [{'customdata': 'Japan'}]}
        )
    ], style={'width': '75%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
       #html.Div(id='repo-stats'
        #   ),
       dcc.Textarea(id='repo-stats',
            rows=20,
            value='',
            placeholder='repository info',
            contentEditable = 'False'
            ),
        #dcc.Graph(id='event-stats'),
        ], style={'display': 'inline-block', 'width': '25%', 'vertical-align':'top', 'padding-top':'8%'}),

    #dcc.Graph(id='barplot-topactivity'),

    #html.Div([
        #html.Div(id='output'),
        #dcc.Graph(
         #   id='graph',
         #   figure={'data': []},
         #   style={'marginTop': '20px'}a
         #dcc.Textarea(id='output',
         #placeholder='repo stat..',
         #value = ''
         #),
    #])

])

labels = ['best', 'good', 'ok']
colors = {'ok':'yellow',
          'good':'blue',
          'best': 'darkgreen'}

def getSliderTime(slider_updatetime):
    today = date.today()
    start_time = today
    if slider_updatetime == 0:
        start_time = today - timedelta(days = 7)
        print("1:", start_time)
    elif slider_updatetime == 10:
        start_time = today - timedelta(days = 30)
        print("2:", start_time)
    elif slider_updatetime == 50:
        start_time = today - timedelta(days = 90)
        print("3:", start_time)
    elif slider_updatetime == 100:
        start_time = today - timedelta(days = 180)
        print("4:", start_time)
    return start_time

#call back function for updating the barplot
@app.callback(
    Output('barplot-topactivity', 'figure'),
    [Input('activity', 'value'),
     Input('slider_updatetime', 'value')])
def update_graph(activity, slider_updatetime):
    # Get DATA from postgres
    print(slider_updatetime)
    postgres_host = 'ec2-54-214-118-72.us-west-2.compute.amazonaws.com'  #os.environ['POSTGRES_HOST']
    postgres_username = 'db_select'  #os.environ['POSTGRES_USERNAME']
    postgres_password = 'db_select' #os.environ['POSTGRES_PASSWORD']
    connection = psycopg2.connect(host=postgres_host,
                                  port ='5432',
                                  dbname='insightproject',
                                  user=postgres_username,
                                  password=postgres_password)
    cursor = connection.cursor()
    table_name = activity + 'event_table'
    #today = date.today()
    start_time = getSliderTime(slider_updatetime)
    y_label = "label"
    if activity == 'aggregate':
        print("aggregated")
        y_label = "aggregate score"
        cursor.execute("select repo_name, sum(score)  from {} where create_date >= \'{}\' group by repo_name ORDER BY sum(score) desc LIMIT 20".format(table_name, start_time))
    else:
        print("not aggregate")
        y_label = "no. of " + activity.lower() + " events"
        y_label = "no. of " + activity.lower() + " events"
        cursor.execute("select repo_name, sum(count)  from {} where create_date >= \'{}\' group by repo_name ORDER BY sum(count) desc LIMIT 20".format(table_name, start_time))
    data = cursor.fetchall()
    #grab repo names and scores from data list
    repo_list = []
    hover_list = []
    count_list = []
    str1 = '<a href=https://github.com/'
    str2 = '</a>'
    for i in range(len(data)):
        hover_list.append(data[i][0])
        repo_list.append(data[i][0])
        #print(repo_list[i])
        count_list.append(data[i][1])
    #max_list = max(count_list)
    #min_list = min(count_list)
    #diff = max_list - min_list
    #one_third = (diff/3) + min_list
    #two_third = (diff/3) + one_third
    #print("repo length=", len(repo_list))
    #bins = [min_list, one_third, two_third, max_list+1]
    #df = pd.DataFrame({'y':count_list,
    #                    'x':repo_list,
    #                    'label':pd.cut(count_list, bins=bins, labels=labels)})
    #df.head()
    return {
        'data': [go.Bar(
            x=repo_list,
            y=count_list,
            #text=repo_list,
            opacity= 0.5,
            marker={'color':'#5042f4'}

        )],
        'layout': go.Layout(
            #title='20 Most popular repositories based on Github {} Event activity'.format(activity),
            #xaxis={'title': 'Time Axis'},
            yaxis={'title': '{}'.format(y_label)},
        #    margin={'l': 60, 'b': 150, 't': 50, 'r': 60},
            legend={'x': 0, 'y': 1},
            hovermode='closest',
            #paper_bgcolor='rgb(173,216,230)',
            plot_bgcolor='rgb(240,248,255)'

        )

    }

@app.callback(
    Output('repo-stats', 'value'),
    [Input('barplot-topactivity', 'clickData'),
     Input('slider_updatetime', 'value')])
def display_click_data(clickData, slider_updatetime):
        #return json.dumps(clickData, indent=2)
    if clickData:
        resp = str(clickData)
        print(resp)
        repo_name = resp.split(':')[5].split(',')[0]
        print("repo_name clicked", repo_name)
        postgres_host = 'ec2-54-214-118-72.us-west-2.compute.amazonaws.com'  #os.environ['POSTGRES_HOST']
        postgres_username = 'db_select'  #os.environ['POSTGRES_USERNAME']
        postgres_password = 'db_select' #os.environ['POSTGRES_PASSWORD']
        connection = psycopg2.connect(host=postgres_host,
                                  port ='5432',
                                  dbname='insightproject',
                                  user=postgres_username,
                                  password=postgres_password)
        cursor = connection.cursor()
        start_time = getSliderTime(slider_updatetime)
        cursor.execute("select user_name, sum(count) from repo_users where repo_name = {} AND create_date >= \'{}\' group by user_name ORDER BY sum(count) desc LIMIT 5".format(repo_name, start_time))
        data = cursor.fetchall()
        out_str = 'repository : ' + repo_name + '\n'
        user_list = []
        if len(data) > 0:
            out_str = out_str + 'USERS LIST:'
        for i in range(len(data)):
            user_list.append(data[i][0])
            out_str = out_str + data[i][0] + '\n'
        cursor.execute("select * from repo_details where repo_name={} order by create_date desc LIMIT 1".format(repo_name))
        data = cursor.fetchall()
        info = False
        if len(data)>0:
            out_str = out_str + '\n REPOSITORY INFO: \n'
            info = True
            forks = data[0][3]
            out_str = out_str + 'No. of Forks:' + str(data[0][3]) + '\n'
            has_wiki = data[0][4]
            out_str = out_str + 'Has Wiki:' + str(data[0][4]) + '\n'
            languages = data[0][5]
            lstr = "".join(x for x in languages)
            out_str = out_str + 'Languages used:' + lstr + '\n'
            open_issues_count = data[0][6]
            out_str = out_str + 'No. of Open Issues:' + str(data[0][6]) + '\n'
            stars_count = data[0][7]
            out_str = out_str + 'No. of Stars:' + str(data[0][7]) + '\n'
            watchers = data[0][8]
            out_str = out_str + 'No. of Watchers:' + str(data[0][8]) + '<br>'
        #out_str = out_str + 'visit' + '<a href=https://github.com'+repo_name+'>here</a>' +'for more details \n'
        print(user_list)
        if (info):
            print(info, forks, has_wiki, languages)
        else:
            print("NOT FOUND")

        return out_str


if __name__ == '__main__':
    app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})
    app.run_server(debug = True, host='0.0.0.0', port=80)
