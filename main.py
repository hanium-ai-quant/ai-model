import os
import sys
import logging
import argparse
import json
from datetime import datetime

import src

sys.path.append('./src')
sys.path.append('./src/korea_data')
sys.path.append('./src/korea_data/korea_stock_data')
sys.path.append('./src/korea_data/korea_index_data')
from src import settings
from src import data_manager

if __name__ == '__main__':
    # 종목 코드 읽어옴
    sector = 'G4050'
    code_list = src.data_manager.code_from_sector(sector)
    # Argument Parser 생성
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['train', 'test', 'update', 'predict'], default='train')
    parser.add_argument('--name', default=datetime.now().strftime('%Y%m%d'))
    parser.add_argument('--stock_code', nargs='*', default=code_list)
    parser.add_argument('--rl_method', choices=['dqn', 'pg', 'ac', 'a2c', 'a3c', 'monkey'], default='a3c')
    parser.add_argument('--net', choices=['dnn', 'lstm', 'cnn', 'monkey'], default='dnn')
    parser.add_argument('--lr', type=float, default=0.0005)
    parser.add_argument('--discount_factor', type=float, default=0.7)
    parser.add_argument('--balance', type=int, default=100000000)
    args = parser.parse_args()

    # 학습기 파라미터 설정
    output_name = f'{sector}_{args.mode}_{args.name}_{args.rl_method}_{args.net}'
    learning = args.mode in ['train', 'update']
    reuse_models = args.mode in ['test', 'update', 'predict']
    value_network_name = f'{sector}_{args.name}_{args.rl_method}_{args.net}_value.mdl'
    policy_network_name = f'{sector}_{args.name}_{args.rl_method}_{args.net}_policy.mdl'
    start_epsilon = 1 if args.mode in ['train', 'update'] else 0
    num_epoches = 500 if args.mode in ['train', 'update'] else 1
    num_steps = 5 if args.net in ['lstm', 'cnn'] else 1


    # 출력 경로 생성
    output_path = os.path.join('./output', output_name)
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    # 파라미터 기록
    params = json.dumps(vars(args))
    with open(os.path.join(output_path, 'params.json'), 'w') as f:
        f.write(params)

    # 모델 경로 준비
    # 모델 포멧은 TensorFlow는 h5, PyTorch는 pickle
    value_network_path = os.path.join('./models', value_network_name)
    policy_network_path = os.path.join('./models', policy_network_name)

    # 로그 기록 설정
    log_path = os.path.join(output_path, f'{output_name}.log')
    if os.path.exists(log_path):
        os.remove(log_path)
    logging.basicConfig(format='%(message)s')
    logger = logging.getLogger(settings.LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    file_handler = logging.FileHandler(filename=log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.info(params)

    # Backend 설정, 로그 설정을 먼저하고 RLTrader 모듈들을 이후에 임포트해야 함
    from src.learners import ReinforcementLearner, DQNLearner, PolicyGradientLearner, ActorCriticLearner, A2CLearner, A3CLearner

    common_params = {}
    list_stock_code = []
    list_chart_data = []
    list_training_data = []
    list_min_trading_price = []
    list_max_trading_price = []

    for stock_code in args.stock_code:
        # 차트 데이터, 학습 데이터 준비
        chart_data, training_data = data_manager.data_manager(stock_code)
        if chart_data is None or training_data is None:
            print(f'No data: {stock_code}, No learning')
            continue

        assert len(chart_data) >= num_steps

        # 최소/최대 단일 매매 금액 설정
        min_trading_price = 100000
        max_trading_price = 10000000

        # 공통 파라미터 설정
        common_params = {'rl_method': args.rl_method,
                         'net': args.net,
                         'num_steps': num_steps,
                         'lr': args.lr,
                         'balance': args.balance,
                         'num_epoches': num_epoches,
                         'discount_factor': args.discount_factor,
                         'start_epsilon': start_epsilon,
                         'output_path': output_path,
                         'reuse_models': reuse_models}

        # 강화학습 시작
        learner = None
        if args.rl_method != 'a3c':
            common_params.update({'stock_code': stock_code,
                                  'chart_data': chart_data,
                                  'training_data': training_data,
                                  'min_trading_price': min_trading_price,
                                  'max_trading_price': max_trading_price})
            if args.rl_method == 'dqn':
                learner = DQNLearner(**{**common_params,
                                        'value_network_path': value_network_path})
            elif args.rl_method == 'pg':
                learner = PolicyGradientLearner(**{**common_params,
                                                   'policy_network_path': policy_network_path})
            elif args.rl_method == 'ac':
                learner = ActorCriticLearner(**{**common_params,
                                                'value_network_path': value_network_path,
                                                'policy_network_path': policy_network_path})
            elif args.rl_method == 'a2c':
                learner = A2CLearner(**{**common_params,
                                        'value_network_path': value_network_path,
                                        'policy_network_path': policy_network_path})
            elif args.rl_method == 'monkey':
                common_params['net'] = args.rl_method
                common_params['num_epoches'] = 10
                common_params['start_epsilon'] = 1
                learning = False
                learner = ReinforcementLearner(**common_params)
        else:
            list_stock_code.append(stock_code)
            list_chart_data.append(chart_data)
            list_training_data.append(training_data)
            list_min_trading_price.append(min_trading_price)
            list_max_trading_price.append(max_trading_price)

    if args.rl_method == 'a3c':
        learner = A3CLearner(**{
            **common_params,
            'list_stock_code': list_stock_code,
            'list_chart_data': list_chart_data,
            'list_training_data': list_training_data,
            'list_min_trading_price': list_min_trading_price,
            'list_max_trading_price': list_max_trading_price,
            'value_network_path': value_network_path,
            'policy_network_path': policy_network_path})

    assert learner is not None

    if args.mode in ['train', 'test', 'update']:
        learner.run(learning=learning)
        if args.mode in ['train', 'update']:
            learner.save_models()
    elif args.mode == 'predict':
        learner.predict()


