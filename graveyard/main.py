from yaat.main import parse_args, train, predict, plot_prediction

{'train': train, 'predict': predict, 'plot_prediction': plot_prediction}[(args:=parse_args()).cmd](args)

# TODO - scale down the model and try a bunch of combinations to see what works best at predicting
# TODO - predictions are more than just the end of the day
# better datetime handling (pd.to_datetime, timezones, etc)
# TODO - get better schema update mechanism because I just frigged at 6 hour epoch
# TODO - true batch scaling, not instance scaling
# TODO - handle live jobs better, record them in the database
# TODO - test get_live_data
# TODO - remove polygon
# TODO - cleanup database
# TODO - add Autoformer support
# TODO - extract all sensitive info to .env file
# TODO - better setup datbase for testing