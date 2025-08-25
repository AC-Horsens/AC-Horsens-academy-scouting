import pandas as pd
import streamlit as st
from pandas import json_normalize
import ast
import numpy as np
from dateutil import parser
import plotly.express as px
st.set_page_config(layout = 'wide')
leagues = ['U17 Division', 'U19 Division', 'U15 Ligaen', 'U17 Ligaen', 'U19 Ligaen']
league = st.selectbox('Choose league',leagues)

def load_data ():
    events = pd.read_csv(f'{league}_events.csv')
    df_matchstats = pd.read_csv(f'{league}_matchstats.csv')
    df_xg = pd.read_csv(f'{league}_xg.csv')
    df_groundduels = pd.read_csv(f'{league}_groundduels.csv')
    return events, df_matchstats,df_groundduels,df_xg

def Process_data(events,df_xg,df_matchstats,df_groundduels):
    xg = events[['SHORTNAME','MATCHLABEL','SHOTXG']]
    xg['SHOTXG'] = xg['SHOTXG'].astype(float)
    xg = xg.groupby(['SHORTNAME','MATCHLABEL']).sum().reset_index()
    df_scouting = xg.merge(df_matchstats, on=['SHORTNAME', 'MATCHLABEL'], how='inner')
    def calculate_score(df, column, score_column):
        df_unique = df.drop_duplicates(column).copy()
        df_unique.loc[:, score_column] = pd.qcut(df_unique[column], q=10, labels=False, duplicates='drop') + 1
        return df.merge(df_unique[[column, score_column]], on=column, how='left')
    
    def calculate_opposite_score(df, column, score_column):
        df_unique = df.drop_duplicates(column).copy()
        df_unique.loc[:, score_column] = pd.qcut(-df_unique[column], q=10, labels=False, duplicates='drop') + 1
        return df.merge(df_unique[[column, score_column]], on=column, how='left')
    minutter_kamp = 30
    minutter_total = 160
    
    df_matchstats = df_matchstats[['SHORTNAME','TEAMNAME','MATCHLABEL','POSITION1CODE','MINUTESONFIELD','SUCCESSFULPASSESTOFINALTHIRD_AVERAGE','FIELDAERIALDUELSWON_PERCENT','NEWSUCCESSFULDRIBBLES_PERCENT','SUCCESSFULTHROUGHPASSES_AVERAGE','DUELSWON_PERCENT','SUCCESSFULPASSESTOFINALTHIRD_PERCENT','XGASSIST','SUCCESSFULCROSSES_AVERAGE','SUCCESSFULPROGRESSIVEPASSES_AVERAGE','PROGRESSIVERUN','ACCELERATIONS','SUCCESSFULPASSES_PERCENT','BALLRECOVERIES','INTERCEPTIONS','DEFENSIVEDUELS','SUCCESSFULDEFENSIVEACTION','FORWARDPASSES','SUCCESSFULFORWARDPASSES_AVERAGE','TOUCHINBOX','XGSHOT','SUCCESSFULKEYPASSES_AVERAGE','SUCCESSFULATTACKINGACTIONS','SHOTASSISTS','BALLLOSSES']]
    df_scouting = df_xg.merge(df_matchstats,how='right')
    df_scouting = df_groundduels.merge(df_scouting,on=['SHORTNAME','TEAMNAME', 'MATCHLABEL'],how='right').reset_index()
    df_scouting['penAreaEntries_per90&crosses%shotassists'] = ((df_scouting['SUCCESSFULPASSESTOFINALTHIRD_AVERAGE'].astype(float)+df_scouting['SUCCESSFULCROSSES_AVERAGE'].astype(float) + df_scouting['XGASSIST'].astype(float))/ df_scouting['MINUTESONFIELD'].astype(float)) * 90

    df_scouting.fillna(0, inplace=True)
    df_scouting = df_scouting.drop_duplicates(subset=['SHORTNAME', 'TEAMNAME', 'POSITION1CODE','MATCHLABEL'])

    def calculate_match_xg(df_scouting):
        # Calculate the total match_xg for each match_id
        df_scouting['match_xg'] = df_scouting.groupby('MATCHLABEL')['SHOTXG'].transform('sum')
        
        # Calculate the total team_xg for each team in each match
        df_scouting['team_xg'] = df_scouting.groupby(['TEAMNAME', 'MATCHLABEL'])['SHOTXG'].transform('sum')
        
        # Calculate opponents_xg as match_xg - team_xg
        df_scouting['opponents_xg'] = df_scouting['match_xg'] - df_scouting['team_xg']
        df_scouting['opponents_xg'] = pd.to_numeric(df_scouting['opponents_xg'], errors='coerce')
       
        return df_scouting

    df_scouting = calculate_match_xg(df_scouting)
    df_scouting.fillna(0, inplace=True)
    def balanced_central_defender():
        df_balanced_central_defender = df_scouting[df_scouting['POSITION1CODE'].notna() & df_scouting['POSITION1CODE'].str.contains('cb')]
        df_balanced_central_defender['MINUTESONFIELD'] = df_balanced_central_defender['MINUTESONFIELD'].astype(int)
        df_balanced_central_defender = df_balanced_central_defender[df_balanced_central_defender['MINUTESONFIELD'].astype(int) >= minutter_kamp]
        df_balanced_central_defender = calculate_opposite_score(df_balanced_central_defender,'opponents_xg', 'opponents xg score')
        
        df_balanced_central_defender = calculate_score(df_balanced_central_defender,'total_duels', 'total_duels score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender,'stoppedprogress_percent', 'stoppedprogress_percent score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender,'recoveredpossession_percent', 'recoveredpossession_percent score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'DUELSWON_PERCENT', 'DUELSWON_PERCENT score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'INTERCEPTIONS', 'INTERCEPTIONS score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'BALLRECOVERIES', 'ballRecovery score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender,'FIELDAERIALDUELSWON_PERCENT', 'FIELDAERIALDUELSWON_PERCENT score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender,'SUCCESSFULPASSESTOFINALTHIRD_AVERAGE', 'SUCCESSFULPASSESTOFINALTHIRD_AVERAGE score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'SUCCESSFULPASSES_PERCENT', 'SUCCESSFULPASSES_PERCENT score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'SUCCESSFULPASSESTOFINALTHIRD_PERCENT', 'SUCCESSFULPASSESTOFINALTHIRD_PERCENT score')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'SUCCESSFULPROGRESSIVEPASSES_AVERAGE', 'SUCCESSFULPROGRESSIVEPASSES_AVERAGE score')
        df_balanced_central_defender = calculate_opposite_score(df_balanced_central_defender,'BALLLOSSES','BALLLOSSES score')

        df_balanced_central_defender['Defending'] = df_balanced_central_defender[['DUELSWON_PERCENT score','total_duels score','stoppedprogress_percent score','stoppedprogress_percent score','recoveredpossession_percent score','stoppedprogress_percent score','opponents xg score','opponents xg score','FIELDAERIALDUELSWON_PERCENT score', 'INTERCEPTIONS score', 'INTERCEPTIONS score', 'ballRecovery score']].mean(axis=1)
        df_balanced_central_defender['Possession value added'] = df_balanced_central_defender[['SUCCESSFULPASSESTOFINALTHIRD_AVERAGE score','SUCCESSFULPASSESTOFINALTHIRD_PERCENT score','SUCCESSFULPROGRESSIVEPASSES_AVERAGE score','SUCCESSFULPROGRESSIVEPASSES_AVERAGE score','SUCCESSFULPROGRESSIVEPASSES_AVERAGE score','BALLLOSSES score']].mean(axis=1)
        df_balanced_central_defender['Passing'] = df_balanced_central_defender[['SUCCESSFULPASSES_PERCENT score', 'SUCCESSFULPASSES_PERCENT score','SUCCESSFULPASSESTOFINALTHIRD_PERCENT score']].mean(axis=1)

        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'Defending', 'Defending_')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'Passing', 'Passing_')
        df_balanced_central_defender = calculate_score(df_balanced_central_defender, 'Possession value added', 'Possession_value_added')


        df_balanced_central_defender['Total score'] = df_balanced_central_defender[['Defending_','Defending_','Possession_value_added','Passing_']].mean(axis=1)

        df_balanced_central_defender = df_balanced_central_defender[['SHORTNAME','TEAMNAME','POSITION1CODE','MATCHLABEL','MINUTESONFIELD','Defending_','Possession_value_added','Passing_','Total score']]
        
        df_balanced_central_defendertotal = df_balanced_central_defender[['SHORTNAME','TEAMNAME','POSITION1CODE','MINUTESONFIELD','Defending_','Possession_value_added','Passing_','Total score']]
        df_balanced_central_defendertotal = df_balanced_central_defendertotal.groupby(['SHORTNAME','TEAMNAME','POSITION1CODE']).mean().reset_index()
        minutter = df_balanced_central_defender.groupby(['SHORTNAME', 'TEAMNAME','POSITION1CODE'])['MINUTESONFIELD'].sum().astype(float).reset_index()
        df_balanced_central_defendertotal['MINUTESONFIELD total'] = minutter['MINUTESONFIELD']
        with st.expander('Game by game'):
            df_balanced_central_defender = df_balanced_central_defender.sort_values('Total score',ascending = False)
            st.dataframe(df_balanced_central_defender,hide_index=True)
        
        with st.expander('Total'):       
            df_balanced_central_defendertotal = df_balanced_central_defendertotal[['SHORTNAME','TEAMNAME','POSITION1CODE','MINUTESONFIELD','Defending_','Possession_value_added','Passing_','Total score']]
            aggregation_functions = { 
                'MINUTESONFIELD total': 'sum'  # Sum for minutesonfield
            }

            # Add 'mean' for all other columns except the groupby columns and 'minutesonfield'
            for col in df_balanced_central_defendertotal.columns:
                if col not in ['SHORTNAME','TEAMNAME', 'MINUTESONFIELD total']:
                    aggregation_functions[col] = 'mean'

            # Apply the groupby with the specified aggregation functions
            df_balanced_central_defendertotal = df_balanced_central_defendertotal.groupby(['SHORTNAME','TEAMNAME']).agg(aggregation_functions).reset_index()
            df_balanced_central_defendertotal = df_balanced_central_defendertotal[df_balanced_central_defendertotal['MINUTESONFIELD total'].astype(int) >= minutter_total]
            df_balanced_central_defendertotal = df_balanced_central_defendertotal.sort_values('Total score',ascending = False)
            st.dataframe(df_balanced_central_defendertotal,hide_index=True)
        return df_balanced_central_defender
    
    def fullbacks():
        st.title('FB')
        df_backs = df_scouting[(df_scouting['position1code'].str.contains('rb') |df_scouting['position1code'].str.contains('lb') |df_scouting['position1code'].str.contains('lwb') |df_scouting['position1code'].str.contains('rwb'))]        
        df_backs['minutesonfield'] = df_backs['minutesonfield'].astype(int)
        df_backs = df_backs[df_backs['minutesonfield'].astype(int) >= minutter_kamp]

        #df_backs = calculate_score(df_backs,'totalDuels', 'totalDuels score')
        #df_backs = calculate_score(df_backs,'stoppedProgressPercentage', 'stoppedProgressPercentage score')
        #df_backs = calculate_score(df_backs,'recoveredPossessionPercentage', 'recoveredPossessionPercentage score')
        df_backs = calculate_opposite_score(df_backs,'opponents_xg', 'opponents xg score')
        df_backs = calculate_score(df_backs,'successfulattackingactions', 'Possession value added score')
        df_backs = calculate_score(df_backs, 'newduelswon_percent', 'percent_duelsWon score')
        df_backs = calculate_score(df_backs, 'successfulpassestofinalthird_percent', 'percent_successfulPassesToFinalThird score')
        df_backs = calculate_score(df_backs, 'penAreaEntries_per90&crosses%shotassists', 'Penalty area entries & crosses & shot assists score')
        df_backs = calculate_score(df_backs, 'shotassists', 'average_shotAssists score')
        df_backs = calculate_score(df_backs, 'interceptions', 'interception_per90 score')
        df_backs = calculate_score(df_backs, 'interceptions', 'average_interceptions score')
        df_backs = calculate_score(df_backs, 'successfulpasses_percent', 'percent_successfulPasses score')
        df_backs = calculate_score(df_backs, 'crosses', 'crosses_per90 score')
        df_backs = calculate_score(df_backs, 'progressivepasses', 'average_progressivePasses score')
        df_backs = calculate_score(df_backs, 'successfulprogressivepasses_percent', 'percent_successfulProgressivePasses score')
        df_backs = calculate_score(df_backs, 'successfulpassestofinalthird_average', 'average_successfulPassesToFinalThird score')
        df_backs = calculate_opposite_score(df_backs,'losses','average_losses score')

        df_backs['Defending (Strong 1v1 Abilities)'] = df_backs[['percent_duelsWon score','average_interceptions score','opponents xg score']].mean(axis=1)
        df_backs['Passing (Game intelligence)'] = df_backs[['percent_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','percent_successfulPasses score','percent_successfulPasses score','Possession value added score','average_losses score']].mean(axis=1)
        df_backs['Chance creation (Crossing)'] = df_backs[['Penalty area entries & crosses & shot assists score','crosses_per90 score','crosses_per90 score','percent_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','Possession value added score','Possession value added score']].mean(axis=1)
        df_backs['Possession value added (Game intelligence)'] = df_backs[['average_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','percent_successfulProgressivePasses score','percent_successfulProgressivePasses score','average_losses score']].mean(axis=1)
        
        df_backs = calculate_score(df_backs, 'Defending (Strong 1v1 Abilities)', 'Defending (Strong 1v1 Abilities)_')
        df_backs = calculate_score(df_backs, 'Passing (Game intelligence)', 'Passing (Game intelligence)_')
        df_backs = calculate_score(df_backs, 'Chance creation (Crossing)','Chance creation (Crossing)_')
        df_backs = calculate_score(df_backs, 'Possession value added (Game intelligence)', 'Possession value added (Game intelligence)_')
        
        df_backs['Total score'] = df_backs[['Defending (Strong 1v1 Abilities)_','Defending (Strong 1v1 Abilities)_','Defending (Strong 1v1 Abilities)_','Defending (Strong 1v1 Abilities)_','Passing (Game intelligence)_','Passing (Game intelligence)_','Chance creation (Crossing)_','Chance creation (Crossing)_','Chance creation (Crossing)_','Possession value added (Game intelligence)_','Possession value added (Game intelligence)_','Possession value added (Game intelligence)_']].mean(axis=1)
        df_backs = df_backs[['shortname','teamname','matchlabel','minutesonfield','Defending (Strong 1v1 Abilities)_','Passing (Game intelligence)_','Chance creation (Crossing)_','Possession value added (Game intelligence)_','Total score']]
        df_backs = df_backs.dropna()
        df_backstotal = df_backs[['shortname','teamname','minutesonfield','Defending (Strong 1v1 Abilities)_','Passing (Game intelligence)_','Chance creation (Crossing)_','Possession value added (Game intelligence)_','Total score']]
        df_backstotal = df_backstotal.groupby(['shortname','teamname']).mean().reset_index()
        minutter = df_backs.groupby(['shortname', 'teamname'])['minutesonfield'].sum().astype(float).reset_index()
        df_backstotal['minutesonfield total'] = minutter['minutesonfield']
        with st.expander('Game by game'):
            df_backs = df_backs.sort_values('Total score',ascending = False)
            st.dataframe(df_backs,hide_index=True)
        with st.expander('Total'):
            df_backstotal = df_backstotal[['shortname','teamname','minutesonfield total','Defending (Strong 1v1 Abilities)_','Passing (Game intelligence)_','Chance creation (Crossing)_','Possession value added (Game intelligence)_','Total score']]
            df_backstotal = df_backstotal[df_backstotal['minutesonfield total'].astype(int) >= minutter_total]
            df_backstotal = df_backstotal.sort_values('Total score',ascending = False)
            st.dataframe(df_backstotal,hide_index=True)
        return df_backs
    
    def number6():
        st.title('DM')
        df_sekser = df_scouting[df_scouting['position1code'].str.contains('dmf', na=False)]
        df_sekser['minutesonfield'] = df_sekser['minutesonfield'].astype(int)
        df_sekser = df_sekser[df_sekser['minutesonfield'].astype(int) >= minutter_kamp]

        #df_sekser = calculate_score(df_sekser,'totalDuels', 'totalDuels score')
        #df_sekser = calculate_score(df_sekser,'stoppedProgressPercentage', 'stoppedProgressPercentage score')
        #df_sekser = calculate_score(df_sekser,'recoveredPossessionPercentage', 'recoveredPossessionPercentage score')
        df_sekser = calculate_opposite_score(df_sekser,'opponents_xg', 'opponents xg score')
        df_sekser = calculate_score(df_sekser,'successfulattackingactions', 'Possession value added score')
        df_sekser = calculate_score(df_sekser, 'newduelswon_percent', 'percent_duelsWon score')
        df_sekser = calculate_score(df_sekser, 'successfulpasses_percent', 'percent_successfulPasses score')
        df_sekser = calculate_score(df_sekser, 'interceptions', 'average_interceptions score')
        df_sekser = calculate_score(df_sekser, 'forwardpasses', 'average_forwardPasses score')
        df_sekser = calculate_score(df_sekser, 'successfulforwardpasses_average', 'average_successfulForwardPasses score')
        df_sekser = calculate_score(df_sekser, 'ballrecoveries', 'possWonDef3rd_possWonMid3rd_possWonAtt3rd_per90 score')
        df_sekser = calculate_score(df_sekser, 'successfulpassestofinalthird_percent', 'percent_successfulPassesToFinalThird score')
        df_sekser = calculate_score(df_sekser, 'ballrecoveries', 'ballRecovery score')
        df_sekser = calculate_score(df_sekser, 'successfulpassestofinalthird_average', 'average_successfulPassesToFinalThird score')
        df_sekser = calculate_score(df_sekser, 'successfulprogressivepasses_percent', 'percent_successfulProgressivePasses score')
        df_sekser = calculate_score(df_sekser, 'progressivepasses', 'average_progressivePasses score')
        df_sekser = calculate_opposite_score(df_sekser,'losses','average_losses score')
        
        
        df_sekser['Defending (Breaking Up Play)'] = df_sekser[['percent_duelsWon score','opponents xg score','average_interceptions score','average_interceptions score','ballRecovery score']].mean(axis=1)
        df_sekser['Passing (Press Resistance & Passing)'] = df_sekser[['percent_successfulPasses score','percent_successfulPasses score','percent_successfulPasses score','average_losses score']].mean(axis=1)
        df_sekser['Progressive ball movement (Game intelligence)'] = df_sekser[['average_progressivePasses score','average_forwardPasses score','average_successfulForwardPasses score','percent_successfulPassesToFinalThird score']].mean(axis=1)
        df_sekser['Possession value added (Game intelligence)'] = df_sekser[['average_losses score','average_successfulPassesToFinalThird score','average_progressivePasses score','average_progressivePasses score','percent_successfulPassesToFinalThird score','percent_successfulProgressivePasses score','percent_successfulProgressivePasses score']].mean(axis=1)
        
        df_sekser = calculate_score(df_sekser, 'Defending (Breaking Up Play)', 'Defending (Breaking Up Play)_')
        df_sekser = calculate_score(df_sekser, 'Passing (Press Resistance & Passing)', 'Passing (Press Resistance & Passing)_')
        df_sekser = calculate_score(df_sekser, 'Progressive ball movement (Game intelligence)','Progressive ball movement (Game intelligence)_')
        df_sekser = calculate_score(df_sekser, 'Possession value added (Game intelligence)', 'Possession value added (Game intelligence)_')
        
        df_sekser['Total score'] = df_sekser[['Defending (Breaking Up Play)_', 'Defending (Breaking Up Play)_','Defending (Breaking Up Play)_','Passing (Press Resistance & Passing)_','Passing (Press Resistance & Passing)_','Progressive ball movement (Game intelligence)','Possession value added (Game intelligence)']].mean(axis=1)
        df_sekser = df_sekser[['shortname','teamname','matchlabel','minutesonfield','Defending (Breaking Up Play)_','Passing (Press Resistance & Passing)_','Progressive ball movement (Game intelligence)','Possession value added (Game intelligence)','Total score']]
        df_sekser = df_sekser.dropna()
        df_seksertotal = df_sekser[['shortname','teamname','minutesonfield','Defending (Breaking Up Play)_','Passing (Press Resistance & Passing)_','Progressive ball movement (Game intelligence)','Possession value added (Game intelligence)','Total score']]

        df_seksertotal = df_seksertotal.groupby(['shortname','teamname']).mean().reset_index()
        minutter = df_sekser.groupby(['shortname', 'teamname'])['minutesonfield'].sum().astype(float).reset_index()
        df_seksertotal['minutesonfield total'] = minutter['minutesonfield']
        with st.expander('Game by game'):
            df_sekser = df_sekser.sort_values('Total score',ascending = False)
            st.dataframe(df_sekser,hide_index=True)
        with st.expander('Total'):
            df_seksertotal = df_seksertotal[['shortname','teamname','minutesonfield total','Defending (Breaking Up Play)_','Passing (Press Resistance & Passing)_','Progressive ball movement (Game intelligence)','Possession value added (Game intelligence)','Total score']]
            df_seksertotal= df_seksertotal[df_seksertotal['minutesonfield total'].astype(int) >= minutter_total]
            df_seksertotal = df_seksertotal.sort_values('Total score',ascending = False)
            st.dataframe(df_seksertotal,hide_index=True)
        return df_sekser

    def number8():
        st.title('CM')
        df_otter = df_scouting[df_scouting['position1code'].str.contains('cmf', na=False)]
        df_otter['minutesonfield'] = df_otter['minutesonfield'].astype(int)
        df_otter = df_otter[df_otter['minutesonfield'].astype(int) >= minutter_kamp]

        df_otter = calculate_score(df_otter,'successfulattackingactions','Possession value total score')
        df_otter = calculate_score(df_otter,'successfulattackingactions', 'Possession value score')
        df_otter = calculate_score(df_otter,'successfulattackingactions', 'Possession value added score')
        df_otter = calculate_score(df_otter, 'newduelswon_percent', 'percent_duelsWon score')
        df_otter = calculate_score(df_otter, 'successfulpasses_percent', 'percent_successfulPasses score')
        df_otter = calculate_score(df_otter, 'successfulpassestofinalthird_percent', 'percent_successfulPassesToFinalThird score')
        df_otter = calculate_score(df_otter, 'interceptions', 'average_interceptions score')
        df_otter = calculate_score(df_otter, 'ballrecoveries', 'possWonDef3rd_possWonMid3rd_possWonAtt3rd_per90 score')
        df_otter = calculate_score(df_otter, 'passestofinalthird', 'passestofinalthird score')
        df_otter = calculate_score(df_otter, 'shotassists','average_shotAssists score')
        df_otter = calculate_score(df_otter, 'touchinbox','average_touchInBox score')
        df_otter = calculate_score(df_otter, 'progressivepasses', 'average_progressivePasses score')
        df_otter = calculate_score(df_otter, 'successfulpassestofinalthird_average', 'average_successfulPassesToFinalThird score')
        df_otter = calculate_score(df_otter, 'successfulprogressivepasses_percent', 'percent_successfulProgressivePasses score')


        df_otter['Defending (Breaking Up Play)'] = df_otter[['percent_duelsWon score','possWonDef3rd_possWonMid3rd_possWonAtt3rd_per90 score']].mean(axis=1)
        df_otter['Passing (Game intelligence)'] = df_otter[['percent_successfulPassesToFinalThird score','percent_successfulPasses score']].mean(axis=1)
        df_otter['Progressive ball movement (Break Lines)'] = df_otter[['average_shotAssists score','average_progressivePasses score','average_touchInBox score','percent_successfulPassesToFinalThird score','Possession value total score']].mean(axis=1)
        df_otter['Possession value (Game intelligence)'] = df_otter[['average_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','percent_successfulProgressivePasses score','percent_successfulProgressivePasses score']].mean(axis=1)
        
        df_otter = calculate_score(df_otter, 'Defending (Breaking Up Play)', 'Defending (Breaking Up Play)_')
        df_otter = calculate_score(df_otter, 'Passing (Game intelligence)', 'Passing (Game intelligence)_')
        df_otter = calculate_score(df_otter, 'Progressive ball movement (Break Lines)','Progressive ball movement (Break Lines)_')
        df_otter = calculate_score(df_otter, 'Possession value (Game intelligence)', 'Possession value (Game intelligence)_')
        
        df_otter['Total score'] = df_otter[['Defending (Breaking Up Play)_','Passing (Game intelligence)_','Passing (Game intelligence)_','Progressive ball movement (Break Lines)','Progressive ball movement (Break Lines)','Possession value (Game intelligence)_','Possession value (Game intelligence)_','Possession value (Game intelligence)_']].mean(axis=1)
        df_otter = df_otter[['shortname','teamname','matchlabel','minutesonfield','Defending (Breaking Up Play)_','Passing (Game intelligence)_','Progressive ball movement (Break Lines)','Possession value (Game intelligence)_','Total score']]
        df_otter = df_otter.dropna()

        df_ottertotal = df_otter[['shortname','teamname','minutesonfield','Defending (Breaking Up Play)_','Passing (Game intelligence)_','Progressive ball movement (Break Lines)','Possession value (Game intelligence)_','Total score']]

        df_ottertotal = df_ottertotal.groupby(['shortname','teamname']).mean().reset_index()
        minutter = df_otter.groupby(['shortname', 'teamname'])['minutesonfield'].sum().astype(float).reset_index()
        df_ottertotal['minutesonfield total'] = minutter['minutesonfield']
        with st.expander('Game by game'):
            df_otter = df_otter.sort_values('Total score',ascending = False)
            st.dataframe(df_otter,hide_index=True)
        
        with st.expander('Total'):
            df_ottertotal = df_ottertotal[['shortname','teamname','minutesonfield total','Defending (Breaking Up Play)_','Passing (Game intelligence)_','Progressive ball movement (Break Lines)','Possession value (Game intelligence)_','Total score']]
            df_ottertotal= df_ottertotal[df_ottertotal['minutesonfield total'].astype(int) >= minutter_total]
            df_ottertotal = df_ottertotal.sort_values('Total score',ascending = False)
            st.dataframe(df_ottertotal,hide_index=True)
        return df_otter
        
    def number10():
        st.title('AM')
        df_10 = df_scouting[df_scouting['position1code'].str.contains('amf', na=False)]
        df_10['minutesonfield'] = df_10['minutesonfield'].astype(int)
        df_10 = df_10[df_10['minutesonfield'].astype(int) >= minutter_kamp]
        
        df_10 = calculate_score(df_10,'successfulattackingactions','Possession value total score')
        df_10 = calculate_score(df_10,'successfulattackingactions', 'Possession value score')
        df_10 = calculate_score(df_10,'successfulattackingactions', 'Possession value added score')
        df_10 = calculate_score(df_10, 'successfulpasses_percent', 'percent_successfulPasses score')
        df_10 = calculate_score(df_10, 'successfulpassestofinalthird_average', 'average_successfulPassesToFinalThird score')
        df_10 = calculate_score(df_10, 'successfulpassestofinalthird_percent', 'percent_successfulPassesToFinalThird score')
        df_10 = calculate_score(df_10, 'progressivepasses', 'average_progressivePasses score')
        df_10 = calculate_score(df_10, 'shotassists','average_shotAssists score')
        df_10 = calculate_score(df_10, 'touchinbox','average_touchInBox score')
        df_10 = calculate_score(df_10, 'newsuccessfuldribbles_percent','percent_newSuccessfulDribbles score')
        df_10 = calculate_score(df_10, 'successfulthroughpasses_average','average_throughPasses score')
        df_10 = calculate_score(df_10, 'keypasses','average_keyPasses score')
        df_10 = calculate_score(df_10, 'shotxg','shotxg score')


        df_10['Passing (Game intelligence)'] = df_10[['percent_successfulPassesToFinalThird score','percent_successfulPasses score']].mean(axis=1)
        df_10['Chance creation (Strong 1v1 Abilities / Attacking Third Threat)'] = df_10[['average_shotAssists score','average_touchInBox score','percent_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','Possession value total score','Possession value score','average_progressivePasses score','percent_newSuccessfulDribbles score','average_touchInBox score','average_throughPasses score','average_keyPasses score']].mean(axis=1)
        df_10['Goalscoring ((Attacking Third Threat))'] = df_10[['average_touchInBox score','shotxg score','shotxg score','shotxg score']].mean(axis=1)
        df_10['Possession value (Attacking Third Threat)' ] = df_10[['Possession value total score','Possession value total score','Possession value added score','Possession value score','Possession value score','Possession value score']].mean(axis=1)
                
        df_10 = calculate_score(df_10, 'Passing (Game intelligence)', 'Passing (Game intelligence)_')
        df_10 = calculate_score(df_10, 'Chance creation (Strong 1v1 Abilities / Attacking Third Threat)','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_')
        df_10 = calculate_score(df_10, 'Goalscoring ((Attacking Third Threat))','Goalscoring ((Attacking Third Threat))_')        
        df_10 = calculate_score(df_10, 'Possession value (Attacking Third Threat)', 'Possession value (Attacking Third Threat)_')
        
        df_10['Total score'] = df_10[['Passing (Game intelligence)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)','Goalscoring ((Attacking Third Threat))_','Goalscoring ((Attacking Third Threat))_','Possession value (Attacking Third Threat)_','Possession value (Attacking Third Threat)_']].mean(axis=1)
        df_10 = df_10[['shortname','teamname','matchlabel','minutesonfield','Passing (Game intelligence)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)','Goalscoring ((Attacking Third Threat))_','Possession value (Attacking Third Threat)_','Total score']]
        df_10 = df_10.dropna()
        df_10total = df_10[['shortname','teamname','minutesonfield','Passing (Game intelligence)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)','Goalscoring ((Attacking Third Threat))_','Possession value (Attacking Third Threat)_','Total score']]

        df_10total = df_10total.groupby(['shortname','teamname']).mean().reset_index()
        minutter = df_10.groupby(['shortname', 'teamname'])['minutesonfield'].sum().astype(float).reset_index()
        df_10total['minutesonfield total'] = minutter['minutesonfield']
        with st.expander('Game by game'):
            df_10 = df_10.sort_values('Total score',ascending = False)
            st.dataframe(df_10,hide_index=True)
        
        with st.expander('Total'):
            df_10total = df_10total[['shortname','teamname','minutesonfield total','Passing (Game intelligence)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)','Goalscoring ((Attacking Third Threat))_','Possession value (Attacking Third Threat)_','Total score']]
            df_10total= df_10total[df_10total['minutesonfield total'].astype(int) >= minutter_total]
            df_10total = df_10total.sort_values('Total score',ascending = False)
            st.dataframe(df_10total,hide_index=True)
        return df_10
    
    def winger():
        st.title('Winger')
        df_10 = df_scouting[(df_scouting['position1code'].str.contains('lw')) | (df_scouting['position1code'].str.contains('rw'))| (df_scouting['position1code'].str.contains('lamf'))| (df_scouting['position1code'].str.contains('ramf'))] 
        df_10['minutesonfield'] = df_10['minutesonfield'].astype(int)
        df_10 = df_10[df_10['minutesonfield'].astype(int) >= minutter_kamp]

        df_10 = calculate_score(df_10,'successfulattackingactions','Possession value total score')
        df_10 = calculate_score(df_10,'successfulattackingactions', 'Possession value score')
        df_10 = calculate_score(df_10,'successfulattackingactions', 'Possession value added score')
        df_10 = calculate_score(df_10,'progressiverun', 'progressiveRun score')
        df_10 = calculate_score(df_10, 'successfulpasses_percent', 'percent_successfulPasses score')
        df_10 = calculate_score(df_10, 'successfulpassestofinalthird_average', 'average_successfulPassesToFinalThird score')
        df_10 = calculate_score(df_10, 'successfulpassestofinalthird_percent', 'percent_successfulPassesToFinalThird score')
        df_10 = calculate_score(df_10, 'progressivepasses', 'average_progressivePasses score')
        df_10 = calculate_score(df_10, 'shotassists','average_shotAssists score')
        df_10 = calculate_score(df_10, 'touchinbox','average_touchInBox score')
        df_10 = calculate_score(df_10, 'newsuccessfuldribbles_percent','percent_newSuccessfulDribbles score')
        df_10 = calculate_score(df_10, 'successfulthroughpasses_average','average_throughPasses score')
        df_10 = calculate_score(df_10, 'keypasses','average_keyPasses score')
        df_10 = calculate_score(df_10, 'shotxg','shotxg score')


        df_10['Passing (Game intelligence)'] = df_10[['percent_successfulPassesToFinalThird score','percent_successfulPasses score']].mean(axis=1)
        df_10['Chance creation (Explosive Dribbler) / Attacking Third Threat'] = df_10[['progressiveRun score','average_shotAssists score','average_touchInBox score','percent_successfulPassesToFinalThird score','percent_successfulPassesToFinalThird score','Possession value total score','Possession value score','percent_newSuccessfulDribbles score','percent_newSuccessfulDribbles score','percent_newSuccessfulDribbles score','average_touchInBox score','average_throughPasses score','average_keyPasses score','average_keyPasses score','average_keyPasses score']].mean(axis=1)
        df_10['Goalscoring (Attacking Third Threat)'] = df_10[['touchinbox','shotxg score','shotxg score','shotxg score']].mean(axis=1)
        df_10['Possession value (Game intelligence)'] = df_10[['Possession value total score','Possession value total score','Possession value added score','Possession value score','Possession value score','Possession value score']].mean(axis=1)
                
        df_10 = calculate_score(df_10, 'Passing (Game intelligence)', 'Passing (Game intelligence)_')
        df_10 = calculate_score(df_10, 'Chance creation (Explosive Dribbler) / Attacking Third Threat','Chance creation (Explosive Dribbler) / Attacking Third Threat)_')
        df_10 = calculate_score(df_10, 'Goalscoring (Attacking Third Threat)','Goalscoring ((Attacking Third Threat))_')        
        df_10 = calculate_score(df_10, 'Possession value (Game intelligence)', 'Possession value (Game intelligence)_')
        
        df_10['Total score'] = df_10[['Passing (Game intelligence)_','Chance creation (Explosive Dribbler) / Attacking Third Threat)_','Chance creation (Explosive Dribbler) / Attacking Third Threat)_','Chance creation (Explosive Dribbler) / Attacking Third Threat)_','Chance creation (Explosive Dribbler) / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Goalscoring ((Attacking Third Threat))_','Goalscoring ((Attacking Third Threat))_','Goalscoring ((Attacking Third Threat))_','Possession value (Game intelligence)_','Possession value (Game intelligence)_','Possession value (Game intelligence)_','Possession value (Game intelligence)_']].mean(axis=1)
        df_10 = df_10[['shortname','teamname','matchlabel','minutesonfield','Passing (Game intelligence)_','Chance creation (Explosive Dribbler) / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Possession value (Game intelligence)_','Total score']]
        df_10 = df_10.dropna()
        df_10total = df_10[['shortname','teamname','minutesonfield','Passing (Game intelligence)_','Chance creation (Explosive Dribbler) / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Possession value (Game intelligence)_','Total score']]

        df_10total = df_10total.groupby(['shortname','teamname']).mean().reset_index()
        minutter = df_10.groupby(['shortname', 'teamname'])['minutesonfield'].sum().astype(float).reset_index()
        df_10total['minutesonfield total'] = minutter['minutesonfield']
        with st.expander('Game by game'):
            df_10 = df_10.sort_values('Total score',ascending = False)
            st.dataframe(df_10,hide_index=True)
        
        with st.expander('Total'):
            df_10total = df_10total[['shortname','teamname','minutesonfield total','Passing (Game intelligence)_','Chance creation (Explosive Dribbler) / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Possession value (Game intelligence)_','Total score']]
            df_10total= df_10total[df_10total['minutesonfield total'].astype(int) >= minutter_total]
            df_10total = df_10total.sort_values('Total score',ascending = False)
            st.dataframe(df_10total,hide_index=True)
        return df_10
    
    def Classic_striker():
        st.title('ST')
        df_striker = df_scouting[df_scouting['position1code'].str.contains('cf', na=False)]
        df_striker['minutesonfield'] = df_striker['minutesonfield'].astype(int)
        df_striker = df_striker[df_striker['minutesonfield'].astype(int) >= minutter_kamp]

        df_striker = calculate_score(df_striker,'successfulattackingactions','Possession value total score')
        df_striker = calculate_score(df_striker,'successfulattackingactions', 'Possession value score')
        df_striker = calculate_score(df_striker,'successfulattackingactions', 'Possession value added score')
        df_striker = calculate_score(df_striker, 'successfulpasses_percent', 'percent_successfulPasses score')
        df_striker = calculate_score(df_striker, 'successfulpassestofinalthird_average', 'average_successfulPassesToFinalThird score')
        df_striker = calculate_score(df_striker, 'successfulpassestofinalthird_percent', 'percent_successfulPassesToFinalThird score')
        df_striker = calculate_score(df_striker, 'progressivepasses', 'average_progressivePasses score')
        df_striker = calculate_score(df_striker, 'shotassists','average_shotAssists score')
        df_striker = calculate_score(df_striker, 'touchinbox','average_touchInBox score')
        df_striker = calculate_score(df_striker, 'newsuccessfuldribbles_percent','newSuccessfulDribbles score')
        df_striker = calculate_score(df_striker, 'keypasses','average_keyPasses score')
        df_striker = calculate_score(df_striker, 'shotxg','shotxg score')


        df_striker['Linkup_play (Dominate Attacking Duels)'] = df_striker[['percent_successfulPassesToFinalThird score','percent_successfulPasses score','Possession value score','average_touchInBox score','average_successfulPassesToFinalThird score']].mean(axis=1)
        df_striker['Chance creation (Strong 1v1 Abilities / Attacking Third Threat)'] = df_striker[['average_touchInBox score','Possession value total score','average_touchInBox score','average_successfulPassesToFinalThird score']].mean(axis=1)
        df_striker['Goalscoring ((Attacking Third Threat))'] = df_striker[['touchinbox','shotxg score','shotxg score','shotxg score','shotxg score','shotxg score']].mean(axis=1)
        df_striker['Possession_value (Game intelligence)'] = df_striker[['Possession value total score','Possession value score','Possession value score','Possession value score']].mean(axis=1)

        df_striker = calculate_score(df_striker, 'Linkup_play (Dominate Attacking Duels)', 'Linkup_play (Dominate Attacking Duels)_')
        df_striker = calculate_score(df_striker, 'Chance creation (Strong 1v1 Abilities / Attacking Third Threat)','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_')
        df_striker = calculate_score(df_striker, 'Goalscoring ((Attacking Third Threat))','Goalscoring ((Attacking Third Threat))_')        
        df_striker = calculate_score(df_striker, 'Possession_value (Game intelligence)', 'Possession_value (Game intelligence)_')

        
        df_striker['Total score'] = df_striker[['Linkup_play (Dominate Attacking Duels)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Goalscoring ((Attacking Third Threat))_','Possession_value (Game intelligence)_']].mean(axis=1)
        df_striker = df_striker[['shortname','teamname','matchlabel','minutesonfield','Linkup_play (Dominate Attacking Duels)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Possession_value (Game intelligence)_','Total score']]
        df_striker = df_striker.dropna()

        df_strikertotal = df_striker[['shortname','teamname','minutesonfield','Linkup_play (Dominate Attacking Duels)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Possession_value (Game intelligence)_','Total score']]

        df_strikertotal = df_strikertotal.groupby(['shortname','teamname']).mean().reset_index()
        minutter = df_striker.groupby(['shortname', 'teamname'])['minutesonfield'].sum().astype(float).reset_index()
        df_strikertotal['minutesonfield total'] = minutter['minutesonfield']
        with st.expander('Game by game'):
            df_striker = df_striker.sort_values('Total score',ascending = False)
            st.dataframe(df_striker,hide_index=True)
        with st.expander('Total'):
            df_strikertotal = df_strikertotal[['shortname','teamname','minutesonfield total','Linkup_play (Dominate Attacking Duels)_','Chance creation (Strong 1v1 Abilities / Attacking Third Threat)_','Goalscoring ((Attacking Third Threat))_','Possession_value (Game intelligence)_','Total score']]
            df_strikertotal= df_strikertotal[df_strikertotal['minutesonfield total'].astype(int) >= minutter_total]
            df_strikertotal = df_strikertotal.sort_values('Total score',ascending = False)
            st.dataframe(df_strikertotal,hide_index=True)
        return df_striker
    


    overskrifter_til_menu = {
        'CB': balanced_central_defender,
        'WB': fullbacks,
        'DM': number6,
        'CM': number8,
        'AM': number10,
        'W' : winger,
        'ST' : Classic_striker,        
    }

    selected_tabs = st.multiselect("Choose position profile", list(overskrifter_til_menu.keys()))

    for selected_tab in selected_tabs:
        overskrifter_til_menu[selected_tab]()

events,df_matchstats,df_groundduels,df_xg = load_data()

Process_data(events,df_xg,df_matchstats,df_groundduels)
