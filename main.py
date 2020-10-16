import ast
import json
import os
import random
import subprocess
import time
from decimal import Decimal

import numpy as np
import requests
from flask import Flask, request, jsonify

import brute_analysis
import db

app = Flask(__name__)


def generate_bf_points(config, predictions):
    bf_ranges = config['BF_RANGES']

    min_time_hours = bf_ranges['p1'][0]
    max_time_hours = bf_ranges['p1'][1]

    min_sl = bf_ranges['p2'][0]
    max_sl = bf_ranges['p2'][1]

    min_tp = bf_ranges['p3'][0]
    max_tp = bf_ranges['p3'][1]

    min_regr_quantile = bf_ranges['p6'][0]
    max_regr_quantile = bf_ranges['p6'][1]

    min_rev_regr_quantile = bf_ranges['p7'][0]
    max_rev_regr_quantile = bf_ranges['p7'][1]

    quant = np.quantile(predictions['y_pred'].abs(), [min_regr_quantile, max_regr_quantile])
    min_value, max_value = quant[0], quant[1]

    regr_l_threshold = random.uniform(min_value, max_value)
    regr_s_threshold = random.uniform(min_value, max_value)

    quant_rev = np.quantile(predictions['y_pred'].abs(), [min_rev_regr_quantile, max_rev_regr_quantile])
    min_rev_value, max_rev_value = quant_rev[0], quant_rev[1]
    rev_regr = random.uniform(min_rev_value, max_rev_value)

    time = random.randint(min_time_hours * 36, max_time_hours * 36) * 100
    is_TP_amount_coef = np.random.choice(a=(bf_ranges['p4'][0], bf_ranges['p4'][1]), p=(0.8, 0.2))
    if is_TP_amount_coef:
        min_tp_amount_coef = min_tp / min_value
        max_tp_amount_coef = max_tp / max_value
        take_profit = random.uniform(min_tp_amount_coef, max_tp_amount_coef)
        tp_coef = max([regr_l_threshold, regr_s_threshold]) * take_profit
        if tp_coef > max_tp:
            take_profit = max_tp / max(regr_l_threshold, regr_s_threshold)
            stop_loss = random.uniform(min_sl, max_sl)
        else:
            stop_loss = random.uniform(min_sl, max_sl)
    else:
        take_profit = random.uniform(min_tp, max_tp)
        stop_loss = random.uniform(min_sl, max_sl)

    is_trailing_stop = np.random.choice(a=(bf_ranges['p5'][0], bf_ranges['p5'][1]), p=(0.8, 0.2))

    trend_coef = random.uniform(bf_ranges['p8'][0], bf_ranges['p8'][1])
    trend_period = random.randint(bf_ranges['p9'][0], bf_ranges['p9'][1])

    min_hours = bf_ranges['rev_regr_exp_time'][0]
    max_hours = bf_ranges['rev_regr_exp_time'][1]
    reg_rev_expired_time = random.randint(min_hours * 36, max_hours * 36) * 100

    accuracy_filter = np.random.choice(a=(bf_ranges['acc_filter'][0], bf_ranges['acc_filter'][1]),
                                       p=(0.5, 0.5))
    if accuracy_filter == 1:
        accuracy_level = random.uniform(bf_ranges['acc_lvl'][0], bf_ranges['acc_lvl'][1])
        accuracy_field = random.choice(bf_ranges['acc_fields'])
    else:
        accuracy_level = 0
        accuracy_field = "n"

    vol_tpsl_filter = np.random.choice(a=(bf_ranges['vol_tpsl_filter'][0], bf_ranges['vol_tpsl_filter'][1]),
                                       p=(0.5, 0.5))
    if vol_tpsl_filter == 1:
        vol_tp_level = random.uniform(bf_ranges['vol_tp_lvl'][0], bf_ranges['vol_tp_lvl'][1])
        vol_sl_level = random.uniform(bf_ranges['vol_sl_lvl'][0], bf_ranges['vol_sl_lvl'][1])
        vol_tpsl_field = random.choice(bf_ranges['vol_tpsl_fields'])
    else:
        vol_tp_level = 0
        vol_sl_level = 0
        vol_tpsl_field = "n"

    x = Decimal(str(min_value)).as_tuple().exponent * (-1) - len(Decimal(str(min_value)).as_tuple().digits)
    x_rev = Decimal(str(min_rev_value)).as_tuple().exponent * (-1) - len(Decimal(str(min_rev_value)).as_tuple().digits)

    rand_params = 
    /*
------------- UNDER NDA -----------------------
*/

    return 
    /*
------------- UNDER NDA -----------------------
*/

def start_pnl_containers(model_name, experiment_name, config):
    print('---- Deploying pnl containers ----')

    pnl_url = os.environ['PNL_URL']
    container_ids = ['0', '00', '000']
    container_config = {
        'oracle_list': [
            {
                'chart_postfix': model_name.replace('_', '-') + '-' + container_ids[0],
                'base': config['BASE'],
                'pnl':
                    {
                        'batch_ts_start': config['INTERVALS']['1'][0] * 1000,
                        'batch_ts_end': config['INTERVALS']['1'][1] * 1000,
                        'experiment_name': experiment_name
                    }
            },
            {
                'chart_postfix': model_name.replace('_', '-') + '-' + container_ids[1],
                'base': config['BASE'],
                'pnl':
                    {
                        'batch_ts_start': config['INTERVALS']['2'][0] * 1000,
                        'batch_ts_end': config['INTERVALS']['2'][1] * 1000,
                        'experiment_name': experiment_name
                    }
            },
            {
                'chart_postfix': model_name.replace('_', '-') + '-' + container_ids[2],
                'base': config['BASE'],
                'pnl':
                    {
                        'batch_ts_start': config['INTERVALS']['3'][0] * 1000,
                        'batch_ts_end': config['INTERVALS']['3'][1] * 1000,
                        'experiment_name': experiment_name
                    }
            }
        ]
    }

    print('Container config:', container_config)

    db.save_pnl_container_config(container_config)

    print('---- Starting pnl calculations ----')

    bash_cmd = /*
------------- UNDER NDA -----------------------
*/

    subprocess.run(bash_cmd, shell=True)

    for container_id in container_ids:
        is_ready4pnl = False
        url = pnl_url + model_name.replace('_', '-') + '-' + container_id
        t_end = time.time() + (60 * 10)
        while not is_ready4pnl:
            try:
                req = requests.get(url + '/ready4pnls')
                if req.text == model_name:
                    is_ready4pnl = True
                    try:
                        req = requests.get(url + '/pnl')
                        print('PNL URL:', req.url)
                        print('Container ID: ', container_id, ' started!')
                    except Exception as e:
                        print(e)
                        return False
                else:
                    if time.time() < t_end:
                        print('Deploying ...')
                        time.sleep(10)
                    else:
                        print('Could Not deploy PNL container ID: ', container_id)
                        return False
            except Exception as e:
                if time.time() < t_end:
                    print('Deploying...')
                    time.sleep(10)
                else:
                    print(e)
                    print('Could Not deploy PNL container ID: ', container_id)
                    return False

    return True


def check_result_is_ready(model_name):
    print('---- Checking result ----')
    pnl_url = os.environ['PNL_URL']
    container_ids = ['0', '00', '000']

    for container_id in container_ids:
        is_ready = False
        url = pnl_url + model_name.replace('_', '-') + '-' + container_id
        print('PNL URL:', url)
        while not is_ready:
            try:
                r = requests.get(url + '/in_progress')
                if r.text == '0':
                    is_ready = True
                    print(container_id, ' is ready!')
                elif (r.status_code == 400) or (r.status_code == 404) or (r.status_code == 503):
                    print('Something went wrong with container! Error: ', r.status_code)
                    return False
                else:
                    time.sleep(30)
                    print(container_id, ' in progress ...')
            except Exception as e:
                print('Something went wrong with container!!')
                print(e)
                return False

    return True


@app.route("/", methods=['GET'])
def test():
    return "It's Alive!!!"


def choose_basic_points(model_name):
/*
------------- UNDER NDA -----------------------
*/

    return points


@app.route("/start", methods=['GET', 'POST'])
def start_pipeline():
    data = ast.literal_eval(request.data.decode('utf-8'))
    model_names = data.get('model_names')
    experiment_name = data.get('experiment_name', 'test')
    learn_rate = data.get('learn_rate', None)

    print('Model names:', model_names, ' Experiment_name:', experiment_name)
    batchsize = 1

    for i in range(0, len(model_names), batchsize):
        batch = model_names[i:i + batchsize]
        for model_name in batch:
            config = db.get_config(model_name, experiment_name)
            if learn_rate is not None:
                config['LEARN_DISTANCE_LIMIT'] = learn_rate
                config['LEARN_DISTANCE_LIMIT_RED'] = learn_rate
                config['LEARN_DISTANCE_LIMIT_GREEN'] = learn_rate
            print('Config({model_name}):'.format(model_name=model_name), config)

            # ranges = config['INTERVALS'][:-1]
            predictions = db.get_oracle_predictions(date_start=config['INTERVALS']['1'][0],
                                                    date_end=config['INTERVALS']['3'][1],
                                                    model_name=model_name)
            if config.get('BASIC_POINTS', False):
                random_points = choose_basic_points(model_name)
                if random_points is None:
                    continue
            else:
                random_points = [generate_bf_points(config, predictions) for _ in range(config['BF_POINTS_AMOUNT'])]

            db.save_bf_points(model_name, experiment_name, random_points)
            db.add_zero_oracle(model_name, config)

            try:
                is_deployed = start_pnl_containers(model_name, experiment_name, config)
                if not is_deployed:
                    print('Could NOT start pnl container: {model_name}'.format(model_name=model_name))
            except Exception as e:
                print('Could NOT start pnl container: {model_name}'.format(model_name=model_name))
                print(e)
                continue

        for model_name in batch:
            try:
                check_result_is_ready(model_name)
            except Exception as e:
                print(e)

    time.sleep(10)
    best_strategies = brute_analysis.start(model_names, experiment_name)
    if not best_strategies.empty:
        db.save_strategies(best_strategies, config)

    return jsonify(success=True)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
