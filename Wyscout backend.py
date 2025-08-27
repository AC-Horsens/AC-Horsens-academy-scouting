import pandas as pd
import snowflake.connector
import getpass
import shutil
import os

# Snowflake credentials
account = 'NVRWTFN-DB03629'
user = 'SPB@ACHORSENS'
password = 'Seamus10071992'
passcode = input("Enter your current Snowflake MFA code: ")

# Create Snowflake connector connection with MFA
conn = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    passcode=passcode,
    role='KLUB_ACHORSENS_READER',
    warehouse='KLUB_ACHORSENS_WH',
    database='KLUB_ACHORSENS',
    schema='AXIS',
    login_timeout=30,
)

try:
    # Aktive sæsoner
    df_seasons = pd.read_sql("SELECT * FROM WYSCOUT_SEASONS WHERE ACTIVE = 'TRUE'", conn)
    df_seasons.columns = [c.upper() for c in df_seasons.columns]
    season_wyid_list = df_seasons['SEASON_WYID'].tolist()
    competition_wyid_list = ', '.join(map(str, df_seasons['COMPETITION_WYID'].tolist()))
    season_wyid_list_str = ', '.join(map(str, season_wyid_list))

    # Konkurrencer
    df_competitions = pd.read_sql(f"""
        SELECT COMPETITION_WYID, COMPETITIONNAME
        FROM WYSCOUT_COMPETITIONS
        WHERE COMPETITIONNAME IN ('U17 Division','U19 Division','U15 Ligaen','U17 Ligaen','U19 Ligaen','2nd Division','3. Division')
        AND COMPETITION_WYID IN ({competition_wyid_list})
    """, conn)
    df_competitions.columns = [c.upper() for c in df_competitions.columns]
    competition_id = df_competitions['COMPETITION_WYID']
    competition_id_str = ', '.join(map(str, competition_id))

    # Kampe
    df_matches = pd.read_sql(f"""
        SELECT MATCH_WYID, MATCHLABEL, DATE, COMPETITION_WYID, SEASON_WYID
        FROM WYSCOUT_MATCHES
        WHERE SEASON_WYID IN ({season_wyid_list_str})
        AND COMPETITION_WYID IN ({competition_id_str})
    """, conn)
    df_matches.columns = [c.upper() for c in df_matches.columns]
    match_wyid_list = ', '.join(map(str, df_matches['MATCH_WYID'].tolist()))

    # Matchdetails
    df_matchdetails = pd.read_sql(f"""
        SELECT COMPETITION_WYID, MATCH_WYID, TEAM_WYID, PLAYER_WYID
        FROM WYSCOUT_MATCHDETAIL_PLAYERS
        WHERE COMPETITION_WYID IN ({competition_id_str})
        AND MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_matchdetails.columns = [c.upper() for c in df_matchdetails.columns]

    # Events
    df_events = pd.read_sql(f"""
        SELECT *
        FROM WYSCOUT_MATCHEVENTS_COMMON
        WHERE SEASON_WYID IN ({season_wyid_list_str})
        AND COMPETITION_WYID IN ({competition_id_str})
        AND MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_events.columns = [c.upper() for c in df_events.columns]

    # Scouting players
    df_scouting_players = pd.read_sql(f"""
        SELECT PLAYER_WYID, SHORTNAME
        FROM WYSCOUT_PLAYERS
        WHERE COMPETITION_WYID IN ({competition_id_str})
        AND SEASON_WYID IN ({season_wyid_list_str})
    """, conn)
    df_scouting_players.columns = [c.upper() for c in df_scouting_players.columns]

    players_list = ', '.join(map(str, df_scouting_players['PLAYER_WYID'].tolist()))

    # Scouting average
    df_scouting_average = pd.read_sql(f"""
        SELECT *
        FROM WYSCOUT_MATCHADVANCEDPLAYERSTATS_AVERAGE
        WHERE COMPETITION_WYID IN ({competition_id_str})
        AND PLAYER_WYID IN ({players_list})
        AND MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_scouting_average.columns = [c.upper() for c in df_scouting_average.columns]

    # Scouting percent
    df_scouting_percent = pd.read_sql(f"""
        SELECT *
        FROM WYSCOUT_MATCHADVANCEDPLAYERSTATS_PERCENT
        WHERE COMPETITION_WYID IN ({competition_id_str})
        AND PLAYER_WYID IN ({players_list})
        AND MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_scouting_percent.columns = [c.upper() for c in df_scouting_percent.columns]

    # Scouting total
    df_scouting_total = pd.read_sql(f"""
        SELECT MATCH_WYID, PLAYER_WYID, MINUTESONFIELD
        FROM WYSCOUT_MATCHADVANCEDPLAYERSTATS_TOTAL
        WHERE COMPETITION_WYID IN ({competition_id_str})
        AND PLAYER_WYID IN ({players_list})
        AND MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_scouting_total.columns = [c.upper() for c in df_scouting_total.columns]

    # Scouting position
    df_scouting_position = pd.read_sql(f"""
        SELECT MATCH_WYID, PLAYER_WYID, POSITION1NAME, POSITION1CODE, POSITION1PERCENT, POSITION2NAME, POSITION2CODE, POSITION2PERCENT, POSITION3NAME, POSITION3CODE, POSITION3PERCENT
        FROM WYSCOUT_MATCHADVANCEDPLAYERSTATS_BASE
        WHERE COMPETITION_WYID IN ({competition_id_str})
        AND PLAYER_WYID IN ({players_list})
    """, conn)
    df_scouting_position.columns = [c.upper() for c in df_scouting_position.columns]

    # Secondary events
    df_secondary_events = pd.read_sql(f"""
        SELECT MATCH_WYID, EVENT_WYID, PRIMARYTYPE, SECONDARYTYPE1, SECONDARYTYPE2,SECONDARYTYPE3,SECONDARYTYPE4,SECONDARYTYPE5,SECONDARYTYPE6,SECONDARYTYPE7,SECONDARYTYPE8,SECONDARYTYPE9,SECONDARYTYPE10
        FROM WYSCOUT_MATCHEVENTS_SECONDARYTYPE
        WHERE COMPETITION_WYID IN ({competition_id_str})
    """, conn)
    df_secondary_events.columns = [c.upper() for c in df_secondary_events.columns]

    # Teams
    df_teams = pd.read_sql(f"""
        SELECT TEAM_WYID, TEAMNAME
        FROM WYSCOUT_TEAMS
    """, conn)
    df_teams.columns = [c.upper() for c in df_teams.columns]

    # xG, carries, groundduel, passes, possessions
    df_xg = pd.read_sql(f"""
        SELECT EVENT_WYID, MATCH_WYID, PRIMARYTYPE, SHOTISGOAL, SHOTPOSTSHOTXG, SHOTXG
        FROM WYSCOUT_MATCHEVENTS_SHOTS
        WHERE MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_xg.columns = [c.upper() for c in df_xg.columns]

    df_carries = pd.read_sql(f"""
        SELECT *
        FROM WYSCOUT_MATCHEVENTS_CARRY
        WHERE MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_carries.columns = [c.upper() for c in df_carries.columns]
    df_carries = df_carries.drop(columns=['COMPETITION_WYID'], errors='ignore')

    df_groundduel = pd.read_sql(f"""
        SELECT *
        FROM WYSCOUT_MATCHEVENTS_GROUNDDUEL
        WHERE MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_groundduel.columns = [c.upper() for c in df_groundduel.columns]
    df_groundduel = df_groundduel.drop(columns=['COMPETITION_WYID'], errors='ignore')

    df_passes = pd.read_sql(f"""
        SELECT *
        FROM WYSCOUT_MATCHEVENTS_PASSES
        WHERE MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_passes.columns = [c.upper() for c in df_passes.columns]
    df_passes = df_passes.drop(columns=['COMPETITION_WYID'], errors='ignore')

    df_possessions = pd.read_sql(f"""
        SELECT COMPETITION_WYID, MATCH_WYID, EVENT_WYID, PRIMARYTYPE, POSSESSIONTYPE1, POSSESSIONTYPE2, POSSESSIONTYPE3, POSSESSIONTYPE4, POSSESSIONTYPE5, ATTACKXG
        FROM WYSCOUT_MATCHEVENTS_POSSESSIONTYPES
        WHERE MATCH_WYID IN ({match_wyid_list})
    """, conn)
    df_possessions.columns = [c.upper() for c in df_possessions.columns]
    df_possessions = df_possessions.drop(columns=['COMPETITION_WYID'], errors='ignore')

    # Data manipulation/merges (all upper case now)
    df_scouting_players = df_scouting_players[['PLAYER_WYID','SHORTNAME']]

    df_scouting_average = df_scouting_average.merge(df_scouting_players, how='left', on=['PLAYER_WYID'])
    df_scouting_average = df_scouting_average.merge(df_competitions, how='left', on=['COMPETITION_WYID'])
    df_scouting_average = df_scouting_average.merge(df_matchdetails, how='left', on=['COMPETITION_WYID','MATCH_WYID','PLAYER_WYID'])
    df_scouting_average = df_scouting_average.merge(df_matches,how='left',on='MATCH_WYID')
    df_scouting_average = df_scouting_average.merge(df_teams, how='left', on=['TEAM_WYID'])
    df_scouting_average = df_scouting_average.merge(
        df_scouting_percent, 
        how='left', 
        on=['MATCH_WYID', 'PLAYER_WYID'], 
        suffixes=('_AVERAGE', '_PERCENT')
    )
    df_scouting_average = df_scouting_average.merge(df_scouting_position, how='left',on=['MATCH_WYID','PLAYER_WYID'])
    df_scouting_average = df_scouting_average.merge(df_scouting_total,how='left',on=['MATCH_WYID','PLAYER_WYID'])
    df_scouting_average = df_scouting_average.drop_duplicates(subset=['PLAYER_WYID', 'MATCH_WYID'])
    print('Scouting file created')

    # Events merges (all upper case)
    df_events = df_events.merge(df_competitions, how='left', on=['COMPETITION_WYID'])
    df_events = df_events.merge(df_matches,how='left',on=['MATCH_WYID','COMPETITION_WYID'])
    df_events = df_events.merge(df_matchdetails, how='left', on=['COMPETITION_WYID','MATCH_WYID','TEAM_WYID','PLAYER_WYID'])
    df_events = df_events.merge(df_scouting_players,how='left', on=['PLAYER_WYID'])
    df_events = df_events.merge(df_xg,how = 'left', on= ['MATCH_WYID','EVENT_WYID','PRIMARYTYPE'])
    df_events = df_events.merge(df_groundduel,how='left', on= ['MATCH_WYID','EVENT_WYID','PRIMARYTYPE'])
    df_events = df_events.merge(df_carries,how='left', on= ['MATCH_WYID','EVENT_WYID','PRIMARYTYPE'])
    df_events = df_events.merge(df_passes,how='left', on= ['MATCH_WYID','EVENT_WYID','PRIMARYTYPE'])
    df_events = df_events.merge(df_possessions,how='left', on= ['MATCH_WYID','EVENT_WYID','PRIMARYTYPE'])
    df_events = df_events.merge(df_secondary_events,how='left',on=['MATCH_WYID','EVENT_WYID','PRIMARYTYPE'])
    columns_to_drop = ['MATCHTIMESTAMP','VIDEOTIMESTAMP','RELATEDEVENT_WYID','OPPONENTTEAM_WYID']
    df_events = df_events.drop(columns=columns_to_drop, errors='ignore')
    df_events = df_events.drop_duplicates(subset=['MATCH_WYID','EVENT_WYID'])

    leagues = ['U17 Division','U19 Division','U15 Ligaen','U17 Ligaen','U19 Ligaen','2nd Division','3. Division']
    for league in leagues:
        # Filter the DataFrame for the current league
        df_league_events = df_events[df_events['COMPETITIONNAME'] == league]
        df_league_events = df_league_events.merge(df_teams,how='left',on=['TEAM_WYID'])
        df_league_events = df_league_events.drop_duplicates(subset=['MATCH_WYID','EVENT_WYID'])
        df_xg = df_league_events[df_league_events['SHOTXG'] > 0]
        df_xg.to_csv(f'{league}_xg.csv',index=False)
        if 'GROUNDDUELOPPONENT_WYID' in df_league_events.columns:
            df_groundduels = df_league_events[df_league_events['GROUNDDUELOPPONENT_WYID'] > 0]
            df_groundduels = df_groundduels[['MATCH_WYID','PLAYER_WYID','PRIMARYTYPE', 'TEAM_WYID','MATCHLABEL', 'DATE',
                'SHORTNAME', 'GROUNDDUELDUELTYPE','STOPPEDPROGRESS', 'RECOVEREDPOSSESSION','TEAMNAME']]
            df_groundduels = df_groundduels[df_groundduels['GROUNDDUELDUELTYPE'].str.contains('defensive_duel', na=False)]
            df_groundduels['STOPPEDPROGRESS'] = df_groundduels['STOPPEDPROGRESS'] == True
            df_groundduels['RECOVEREDPOSSESSION'] = df_groundduels['RECOVEREDPOSSESSION'] == True

            # Group by player and match
            grouped = df_groundduels.groupby(['SHORTNAME','TEAMNAME', 'MATCHLABEL'])

            # Calculate percentages
            result = grouped.agg(
                total_duels=('GROUNDDUELDUELTYPE', 'count'),
                stoppedprogress_count=('STOPPEDPROGRESS', 'sum'),
                recoveredpossession_count=('RECOVEREDPOSSESSION', 'sum')
            ).reset_index()

            # Calculate percentage metrics
            result['stoppedprogress_percent'] = (result['stoppedprogress_count'] / result['total_duels']) * 100
            result['recoveredpossession_percent'] = (result['recoveredpossession_count'] / result['total_duels']) * 100

            # Optional: Round for readability
            result['stoppedprogress_percent'] = result['stoppedprogress_percent'].round(2)
            result['recoveredpossession_percent'] = result['recoveredpossession_percent'].round(2)

            # Show result
            result.to_csv(f'{league}_groundduels.csv',index=False)
        # Save the filtered DataFrame to a CSV file named 'league_name_events.csv'
        df_league_events.to_csv(f'{league}_events.csv', index=False)

        df_scouting_average_leagues = df_scouting_average[df_scouting_average['COMPETITIONNAME'] == league]
        df_scouting_average_leagues.to_csv(f'{league}_matchstats.csv')

    print('All files created')


finally:
    conn.close()

u19_target_dir = r'C:/Users/Seamus-admin/Documents/GitHub/AC-Horsens-U19'
u19_identifier = "U19 Ligaen_"  # Use the exact format you save with

# Create target folder if it doesn't exist
os.makedirs(u19_target_dir, exist_ok=True)

# Loop over files in current directory and copy U19 Ligaen files
for filename in os.listdir():
    if filename.endswith(".csv") and u19_identifier in filename:
        source_path = os.path.join(os.getcwd(), filename)
        dest_path = os.path.join(u19_target_dir, filename)
        shutil.copyfile(source_path, dest_path)  # <--- This line actually performs the copy
        print(f"Copied {filename} to {u19_target_dir}")

u17_target_dir = r'C:/Users/Seamus-admin/Documents/GitHub/AC-Horsens-U17'
u17_identifier = "U17 Ligaen_"  # Use the exact format you save with

# Create target folder if it doesn't exist
os.makedirs(u17_target_dir, exist_ok=True)

# Loop over files in current directory and copy U19 Ligaen files
for filename in os.listdir():
    if filename.endswith(".csv") and u17_identifier in filename:
        source_path = os.path.join(os.getcwd(), filename)
        dest_path = os.path.join(u17_target_dir, filename)
        shutil.copyfile(source_path, dest_path)  # <--- This line actually performs the copy
        print(f"Copied {filename} to {u17_target_dir}")

u15_target_dir = r'C:/Users/Seamus-admin/Documents/GitHub/AC-Horsens-U15'
u15_identifier = "U15 Ligaen_"  # Use the exact format you save with

# Create target folder if it doesn't exist
os.makedirs(u15_target_dir, exist_ok=True)

# Loop over files in current directory and copy U19 Ligaen files
for filename in os.listdir():
    if filename.endswith(".csv") and u15_identifier in filename:
        source_path = os.path.join(os.getcwd(), filename)
        dest_path = os.path.join(u15_target_dir, filename)
        shutil.copyfile(source_path, dest_path)  # <--- This line actually performs the copy
        print(f"Copied {filename} to {u15_target_dir}")