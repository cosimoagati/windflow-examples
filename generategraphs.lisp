(load (compile-file "plotmetrics.lisp"))

(defparameter *sa-directory*
  (pathname "sa-sentiment-analysis/testresults-2022-08-28-17-03/"))
(defparameter *mo-directory*
  (pathname "mo-machine-outlier/testresults-2022-09-23-20-32/data-stream-top-k/"))
(defparameter *tt-directory*
  (pathname "tt-trending-topics/testresults-2022-08-30-20-53/"))
(defparameter *rl-directory*
  (pathname "rl-reinforcement-learner/testresults-2022-09-01-17-40/"))
(defparameter *lp-directory*
  (pathname "lp-log-processing/testresults-2022-09-02-07-01/"))

(defparameter *parallelism-x-label* "Parallelism degree for each node")
(defparameter *throughput-x-label* "Throughput (tuples per second)")
(defparameter *scalability-y-label* "Scalability")

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-x-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(914581.25 1810770.7 2748242.0 3623771.5 4578529.5 5511013.0 6421764.0 7299102.0 7522324.0 7840571.0 8089628.0 8153471.0 8577864.0 8826029.0 9185283.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(734638.06 1392974.5 2180042.7 2866329.7 3254888.7 4009220.7 4646882.0 5253847.0 5464754.5 5579437.5 5850499.5 6104882.0 6244925.5 6668215.0 6886624.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(990114.6 1997980.2 2970556.2 3909312.2 4890244.0 5934206.5 6777290.0 7746063.0 8197798.5 8507182.0 8975822.0 9314099.0 9673200.0 9995032.0 1.0504489E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1019729.9 2024699.5 3050987.5 3984795.5 5093667.5 6083700.0 7076882.0 8198763.0 8227152.5 8643307.0 9130755.0 9563530.0 9848303.0 1.0319139E+7 1.0710862E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15))))
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput scalability, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-x-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1.0 1.9798905 3.0049186 3.9622195 5.0061483 6.0257225 7.021535 7.980813 8.224883 8.572853 8.845172 8.914977 9.379006 9.650351 10.043157)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(1.0 1.8961371 2.9675057 3.9016895 4.430602 5.4574094 6.325403 7.1516123 7.438703 7.5948114 7.9637847 8.310054 8.500683 9.076871 9.374172)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1.0 2.0179281 3.0002146 3.948343 4.9390683 5.993454 6.8449545 7.8234005 8.279646 8.592118 9.065436 9.407092 9.769777 10.094823 10.609366)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1.0 1.9855253 2.9919565 3.907697 4.995115 5.9659915 6.939957 8.0401325 8.067972 8.476075 8.954092 9.378493 9.657757 10.119483 10.503627)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";Ideal;with points pt 1 ps 2 lc 'black'" #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)) )))
(vgplot:print-plot (pathname "sa-throughput-batching.png"))

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput"
  :x-label *parallelism-x-label*
  :y-label *throughput-x-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(914581.25 1810770.7 2748242.0 3623771.5 4578529.5 5511013.0 6421764.0 7299102.0 7522324.0 7840571.0 8089628.0 8153471.0 8577864.0 8826029.0 9185283.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  ;; #(734638.06 1392974.5 2180042.7 2866329.7 3254888.7 4009220.7 4646882.0 5253847.0 5464754.5 5579437.5 5850499.5 6104882.0 6244925.5 6668215.0 6886624.0)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(914664.2 1774739.9 2730958.5 3580715.7 4391275.5 5344338.0 6155082.0 6930182.0 7269425.5 7536398.0 7836662.0 8325477.0 8538858.0 8818893.0 9254354.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1019729.9 2024699.5 3050987.5 3984795.5 5093667.5 6083700.0 7076882.0 8198763.0 8227152.5 8643307.0 9130755.0 9563530.0 9848303.0 1.0319139E+7 1.0710862E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";Chaining;with linespoints pt 2 ps 1 lc 'black'"
                  #(812227.75 1657455.0 2475656.0 3274093.2 3982319.5 4824067.5 5574321.5 6472990.5 7333856.5 7903270.5 8928637.0 9593692.0 1.0420992E+7 1.137174E+7 1.2060991E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15))))
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(914581.25 1810770.7 2748242.0 3623771.5 4578529.5 5511013.0 6421764.0 7299102.0 7522324.0 7840571.0 8089628.0 8153471.0 8577864.0 8826029.0 9185283.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  ;; #(734638.06 1392974.5 2180042.7 2866329.7 3254888.7 4009220.7 4646882.0 5253847.0 5464754.5 5579437.5 5850499.5 6104882.0 6244925.5 6668215.0 6886624.0)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(914664.2 1774739.9 2730958.5 3580715.7 4391275.5 5344338.0 6155082.0 6930182.0 7269425.5 7536398.0 7836662.0 8325477.0 8538858.0 8818893.0 9254354.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1019729.9 2024699.5 3050987.5 3984795.5 5093667.5 6083700.0 7076882.0 8198763.0 8227152.5 8643307.0 9130755.0 9563530.0 9848303.0 1.0319139E+7 1.0710862E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";Chaining;with linespoints pt 2 ps 1 lc 'black'"
                  #(812227.75 1657455.0 2475656.0 3274093.2 3982319.5 4824067.5 5574321.5 6472990.5 7333856.5 7903270.5 8928637.0 9593692.0 1.0420992E+7 1.137174E+7 1.2060991E+7
                    1.2949175E+7 1.3513249E+7 1.4153548E+7 1.5690533E+7 1.6045256E+7 1.7171888E+7 1.7913572E+7 1.8655708E+7 1.9654012E+7 1.9173312E+7 2.001112E+7 2.0400764E+7
                    2.060529E+7 2.062342E+7 2.0373536E+7 2.0902118E+7 2.1296702E+7 2.0641584E+7 2.231986E+7 2.2142784E+7 2.2398512E+7 2.2338116E+7 2.2891202E+7 2.3340578E+7 2.3752954E+7
                    2.373512E+7 2.3893702E+7 2.4137334E+7 2.4425316E+7 2.4192916E+7 2.4851588E+7 2.5184158E+7 2.5289236E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48)))))

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "SA average latency, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                           #(35.85916 36.297775 35.799652 36.175533 35.78644 35.677677 35.72037 35.905334 40.47396 43.737625 46.728374 50.27041 51.331676 53.13792 54.12379)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                           ;; #(44.61632 47.061497 45.199913 45.757275 50.74786 49.115025 49.440365 50.025368 54.952477 60.29694 63.14271 65.8352 69.812225 70.13883 71.76461)
                           ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                           #(143.30357 147.66386 143.95566 146.3466 149.20023 147.0331 148.91563 151.1991 165.6718 179.0856 189.49045 194.3658 204.10207 211.33687 213.37994)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                           #(528.207 523.59064 527.965 534.7961 533.99646 527.99744 538.958 538.67444 586.2429 632.6912 660.6268 691.1122 716.7674 740.4918 747.95654)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                           #(2033.627 2047.8594 2035.8365 2075.8784 2025.6359 2032.7783 2033.1315 2003.7892 2288.0757 2434.9375 2532.9949 2625.2708 2738.2947 2799.5664 2854.8237)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)))))

(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 4
                              :percentiles '(5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 16
                              :percentiles '(5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 64
                              :percentiles '(5 25 50 75 95 100)))
