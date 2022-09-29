;;;; Copyright (C) 2022 Cosimo Agati

;;;; This program is free software: you can redistribute it and/or
;;;; modify it under the terms of the GNU Affero General Public License
;;;; as published by the Free Software Foundation, either version 3 of
;;;; the License, or (at your option) any later version.

;;;; You should have received a copy of the GNU AGPLv3 with this software,
;;;; if not, please visit <https://www.gnu.org/licenses/>

(declaim (optimize (speed 0) (debug 3) (safety 3)))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (ql:quickload '(:vgplot :yason :alexandria :cl-ppcre :py4cl :cl-csv)))
(py4cl:import-module "matplotlib.pyplot" :as "plt") 

(defpackage plotmetrics
  (:use :common-lisp)
  (:export :plot :make-parameters :make-raw-plot-data :generate-images
           :plot-subplots-from-parameters :plot-subplots-from-raw-data
           :boxplot-subplots :write-triples-to-csv :get-triples :*debug*
           :*y-axis-time-unit-scale* :*time-unit-y-label*))

(in-package plotmetrics)

(defvar *debug* t
  "If non-nil, debug log messages will be printed.")
(defvar *y-axis-time-unit-scale* 1/1000000
  "Whenever the Y axis is a time unit, multiply that value by this amount.")
(defvar *time-unit-y-label* "ms"
  "Time unit to print on the Y axis when showing a time measurement.
If NIL, try to guess the time label automatically.")

(deftype field-to-plot-by () '(member :parallelism :batch-size :chaining))

(deftype plot-comparison-field () '(member :parallelism :batch-size :chaining
                                    :frequency :execmode :timer-nodes-p))

(deftype plot-kind () '(member :normal :scalability :efficiency))

(defclass plot-parameters ()
  ((batch-sizes :type list :accessor batch-sizes :initarg :batch-sizes
                :initform (list 0 1 2 4 8 16 32 64 128))
   (single-batch-size :type fixnum :accessor single-batch-size
                      :initarg :single-batch-size :initform 0)
   (parallelism-degrees :type list :accessor pardegs :initarg :pardegs
                        :initform (list 1 5 10))
   (single-parallelism-degree :type fixnum :accessor single-pardeg
                              :initarg :single-pardeg :initform 1)
   (sampling-rate :type fixnum :accessor sampling-rate :initarg :sampling-rate
                  :initform 100)
   (tuple-generation-rate :type fixnum :accessor tuple-rate
                          :initarg :tuple-rate :initform 0)
   (chaining-p :type boolean :accessor chaining-p :initarg :chaining-p
               :initform nil)
   (execution-mode :type string :accessor execmode :initarg :execmode
                   :initform "default")
   (plot-by :type field-to-plot-by :accessor plot-by :initarg :plot-by
            :initform :parallelism)
   (metric-to-plot :type string :accessor metric :initarg :metric
                   :initform "throughput")
   (percentile :type string :accessor percentile :initarg :percentile
               :initform "mean")
   (percentiles :type list :accessor percentiles :initarg :percentiles
                :initform (list 0 5 25 50 75 95 100))
   (compare-by :type plot-comparison-field :accessor compare-by
               :initarg :compare-by :initform :batch-size)
   (timer-nodes-p :type boolean :accessor timer-nodes-p :initarg :timer-nodes-p
                  :initform t)
   (frequencies :type list :accessor frequencies :initarg :frequencies
                :initform (list 2 4 6 8 10))
   (single-frequency :type fixnum :accessor single-freq :initarg :single-freq
                     :initform 2)
   (directory-to-plot :type (or string pathname) :accessor plotdir
                      :initarg :plotdir :initform "")
   (plot-kind :type plot-kind :accessor plot-kind :initarg :plot-kind
              :initform :normal)
   (print-title-p :type boolean :accessor print-title-p
                  :initarg :print-title-p :initform t)
   (extra-triples :type sequence :accessor extra-triples
                  :initarg :extra-triples :initform nil)))

(defclass raw-plot-data ()
  ((triples :type list :accessor triples :initarg :triples)
   (title :type string :accessor title :initarg :title)
   (x-label :type string :accessor x-label :initarg :x-label)
   (y-label :type string :accessor y-label :initarg :y-label)))

(defun make-raw-plot-data (&rest args)
  (apply #'make-instance 'raw-plot-data args))

(defun make-parameters (&rest args)
  (apply #'make-instance 'plot-parameters args))

(defvar *default-plot-parameters* (make-parameters))

(defun set-plot-by (metric)
  (unless (member metric '(:parallelism :batch-size :chaining))
    (error "Metric must be one of :PARALLELISM, :BATCH-SIZE or :CHAINING"))
  (setf (plot-by *default-plot-parameters*) metric))

(defun set-compare-by (metric)
  (unless (member metric '(:parallelism :batch-size :chaining :execmode))
    (error "Metric must be one of :PARALLELISM, :BATCH-SIZE or :CHAINING"))
  (setf (compare-by *default-plot-parameters*) metric))

(defun set-batch-sizes (batch-sizes)
  (when (some (lambda (b) (not (member b '(0 1 2 4 8 16 32 64 128))))
              batch-sizes)
    (error "Batch size must be either one of 0 1 2 4 8 16 32 64 128"))
  (setf (batch-sizes *default-plot-parameters*) batch-sizes))

(defun set-parallelism-degrees (parallelism-degrees)
  (setf (pardegs *default-plot-parameters*) parallelism-degrees))

(defun set-single-parallelism-degree (parallelism-degree)
  (setf (single-pardeg *default-plot-parameters*) parallelism-degree))

(defun set-sampling-rate (rate)
  (unless (and (integerp rate) (not (minusp rate)))
    (error "Sampling rate must be a non-negative integer"))
  (setf (sampling-rate *default-plot-parameters*) rate))

(defun set-tuple-generation-rate (rate)
  (unless (and (integerp rate) (not (minusp rate)))
    (error "Tuple generation rate must be a non-negative integer"))
  (setf (tuple-rate *default-plot-parameters*) rate))

(defun set-chaining (chaining)
  (setf (chaining-p *default-plot-parameters*) chaining))

(defun set-metric-to-plot (metric)
  (unless (member metric '(:throughput :service-time :latency
                           :total-latency :geo-latency
                           :volume-latency :status-latency))
    (error "Invalid metric"))
  (setf (metric *default-plot-parameters*)
        (string-downcase (symbol-name metric))))

(defun set-percentile (percentile)
  (setf (percentile *default-plot-parameters*) percentile))

(defun set-directory-to-plot (directory)
  (unless (or (pathnamep directory) (stringp directory))
    (error "DIRECTORY must be a pathname or a string designating a path"))
  (setf (plotdir *default-plot-parameters*) directory))

(defun concat (&rest strings)
  (apply #'concatenate 'string strings))

(let ((unit-abbrevs '(("microseconds" . "us")
                      ("microsecond" . "us")
                      ("us" . "us")
                      ("nanoseconds" . "ns")
                      ("nanosecond" . "ns")
                      ("ns" . "ns")
                      ("milliseconds" . "ms")
                      ("millisecond" . "ms")
                      ("ms" . "ms")
                      ("seconds" . "s")
                      ("second" . "s")
                      ("s" . "s"))))
  (defun unit-to-abbrev (unit)
    "Return the time unit abbreviation for UNIT."
    (declare (string unit))
    (cdr (assoc unit unit-abbrevs :test #'string=))))

(let ((scale-factors '(("ns" . 1000000000)
                       ("us" . 1000000)
                       ("ms" . 1000)
                       ("s" . 1))))
  (defun time-unit-scale-factor (time-unit)
    "Return how many of the specified TIME-UNIT are required to form a second."
    (declare ((or symbol string) time-unit))
    (let ((key (if (symbolp time-unit)
                   (string-downcase (symbol-name time-unit))
                   time-unit)))
      (cdr (assoc key scale-factors :test #'equal)))))

(defun base-dirname (directory)
  (second (pathname-directory directory)))

(defun title-from-directory (directory)
  (let* ((words (cl-ppcre:split "-" (base-dirname directory)))
         (acronym (first words))
         (title (concat (second words) " " (third words))))
    (concat (string-capitalize title) " (" (string-upcase acronym) ")")))

(defun transpose-lists (lists)
  (declare (list lists))
  (when (>= (length lists) call-arguments-limit)
    (error "LISTS contains more lists than CALL-ARGUMENTS-LIMIT"))
  (apply #'mapcar #'list lists))

(defun starts-with (word prefix)
  "Return non-NIL if WORD starts with PREFIX, NIL otherwise"
  (declare (string word prefix))
  (let ((prefix-position (search prefix word)))
    (if (numberp prefix-position)
        (zerop prefix-position)
        nil)))

(defun ends-with (word suffix)
  "Return non-NIL if WORD ends with SUFFIX, NIL otherwise."
  (declare (string word suffix))
  (alexandria:ends-with-subseq suffix word :test #'equal))

(defun get-json-objs-from-directory (directory)
  "Return a sequence containing containing pathnames to JSONs in DIRECTORY."
  (let ((file-list (directory (merge-pathnames directory "*"))))
    (map '(vector hash-table) #'yason:parse
         (delete-if-not (lambda (f) (ends-with (namestring f) ".json"))
                        file-list))))

(Defun json-name-match (entry name)
  "Return non-NIL if ENTRY ends with NAME (with some transformations.
Otherwise, return NIL."
  (declare (string entry) (string name))
  (or (ends-with entry name)
      (ends-with (substitute #\Space #\- entry) name)
      (ends-with (substitute #\Space #\- entry) name)))

(defun filter-jsons-by-name (jsons name)
  "Return a sequence containing only the json files whose \"name\" field is NAME"
  (declare (sequence jsons) (string name))
  (remove-if-not (lambda (j) (json-name-match (gethash "name" j) name))
                 jsons))

(defun filter-jsons-by-parallelism (jsons parallelism)
  "Filter out entries in JSONS not matching PARALLELISM.
Return a brand new sequence, the original sequence is left untouched."
  (declare (sequence jsons) (fixnum parallelism))
  (remove-if-not (lambda (j) (= (first (gethash "parallelism" j)) parallelism))
                 jsons))

(defun filter-jsons-by-batch-size (jsons batch-size)
  "Filter out entries in JSONS not matching BATCH-SIZE.
Return a brand new sequence, the original sequence is left untouched."
  (declare (sequence jsons) (fixnum batch-size))
  (let ((batch-good-p
          (if (zerop batch-size)
              (lambda (batch-sizes) (every #'zerop batch-sizes))
              (lambda (batch-sizes) (some (lambda (b) (= b batch-size))
                                          batch-sizes)))))
    (remove-if-not (lambda (j)
                     (funcall batch-good-p (gethash "batch size" j)))
                   jsons)))

(defun filter-jsons-by-chaining (jsons chaining-p)
  "Filter out entries in JSONS not matching CHAINING-P.
Return a brand new sequence, the original sequence is left untouched."
  (declare (sequence jsons) (boolean chaining-p))
  (remove-if-not (lambda (j) (eql (gethash "chaining enabled" j) chaining-p))
                 jsons))

(defun get-sampling-rate (json)
  "Extract tuple sampling rate from the specified JSON object."
  (declare (hash-table json))
  (multiple-value-bind (val1 present1-p) (gethash "sampling_rate" json)
    (multiple-value-bind (val2 present2-p) (gethash "sampling rate" json)
      (if present1-p (values val1 present1-p) (values val2 present2-p)))))

(defun filter-jsons-by-sampling-rate (jsons sampling-rate)
  "Filter out entries in JSONS not matching SAMPLING-RATE.
Return a brand new sequence, the original sequence is left untouched."
  (declare (sequence jsons) (fixnum sampling-rate))
  (remove-if-not (lambda (j) (= (get-sampling-rate j) sampling-rate))
                 jsons))

(defun contains-frequency-fields-p (json)
  (declare (hash-table json))
  (let ((keys (alexandria:hash-table-keys json)))
    (some (lambda (k) (ends-with k "frequency")) keys)))

(defun frequency-fields-equal-value-p (json value)
  (declare (hash-table json) (fixnum value))
  (let ((frequency-keys (remove-if-not (lambda (k) (ends-with k "frequency"))
                                       (alexandria:hash-table-keys json))))
    (every (lambda (v) (equal v value))
           (mapcar (lambda (k) (gethash k json)) frequency-keys))))

(defun filter-jsons-by-frequency (jsons frequency)
  (declare (sequence jsons) (fixnum frequency))
  (remove-if-not (lambda (j) (frequency-fields-equal-value-p j frequency))
                 jsons))

(defun contains-timer-node-key (json)
  (declare (hash-table json))
  (let ((keys (alexandria:hash-table-keys json)))
    (member "using timer nodes" keys :test #'equal)))

(defun contains-timernode-impl-fields-p (json)
  (declare (hash-table json))
  (let ((keys (alexandria:hash-table-keys json)))
    (some (lambda (k) (ends-with k "timer nodes")) keys)))

(defun filter-jsons-by-timernode-impl (jsons timer-nodes-p)
  (declare (sequence jsons) (boolean timer-nodes-p))
  (remove-if-not (lambda (j)
                   (eql (gethash "using timer nodes" j) timer-nodes-p))
                 jsons))

(defun filter-jsons (parameters jsons)
  (declare (plot-parameters parameters) (sequence jsons))
  (with-accessors ((plot-by plot-by) (compare-by compare-by))
      parameters
    (setf jsons (filter-jsons-by-name jsons (metric parameters))
          jsons (filter-jsons-by-sampling-rate jsons (sampling-rate parameters))
          jsons (filter-jsons-by-tuple-rate jsons (tuple-rate parameters)))
    (unless (or (eql plot-by :parallelism) (eql compare-by :parallelism))
      (setf jsons (filter-jsons-by-parallelism jsons
                                               (single-pardeg parameters))))
    (unless (or (eql plot-by :batch-size) (eql compare-by :batch-size))
      (setf jsons (filter-jsons-by-batch-size jsons
                                              (single-batch-size parameters))))
    (unless (or (eql plot-by :chaining) (eql compare-by :chaining))
      (setf jsons (filter-jsons-by-chaining jsons (chaining-p parameters))))
    (when (and (some #'contains-frequency-fields-p jsons)
               (not (eql compare-by :frequency)))
      (setf jsons (filter-jsons-by-frequency jsons (single-freq parameters))))
    (unless (eql compare-by :execmode)
      (setf jsons (filter-jsons-by-execmode jsons (execmode parameters))))
    (when (and (some #'contains-timer-node-key jsons)
               (not (eql compare-by :timer-nodes-p)))
      (setf jsons (filter-jsons-by-timernode-impl jsons
                                                  (timer-nodes-p parameters))))
    (setf jsons (ecase plot-by
                  (:parallelism (sort-jsons-by-parallelism jsons))
                  (:batch-size (sort-jsons-by-batch-size jsons))
                  (:chaining (sort-jsons-by-chaining jsons))))
    jsons))

(defun get-tuple-rate (json)
  "Extract tuple generation rate from the specified JSON object."
  (declare (hash-table json))
  (multiple-value-bind (val1 present1-p) (gethash "tuple_rate" json)
    (multiple-value-bind (val2 present2-p) (gethash "tuple rate" json)
      (if present1-p val1 (values val2 present2-p)))))

(defun filter-jsons-by-tuple-rate (jsons tuple-rate)
  "Filter out entries in JSONS not matching TUPLE-RATE.
Return a brand new sequence, the original sequence is left untouched."
  (declare (sequence jsons) (fixnum tuple-rate))
  (remove-if-not (lambda (j) (= (get-tuple-rate j) tuple-rate))
                 jsons))

(defun filter-jsons-by-execmode (jsons execmode)
  "Filter out entries in JSONS not matching EXECMODE.
Return a brand new sequence, the original sequence is left untouched."
  (declare (sequence jsons) (string execmode))
  (remove-if-not (lambda (j) (string= (gethash "execution mode" j) execmode))
                 jsons))

(defun percentile-to-dictkey (kind)
  "From KIND, return the right string representing the key to index JSONs."
  (declare (string kind))
  (cond ((member kind '("0th" "5th" "25th" "50th" "75th" "95th" "100th")
                 :test #'equal)
         (concat kind " percentile"))
        ((member kind '("0" "5" "25" "50" "75" "95" "100")
                 :test #'equal)
         (concat kind "th percentile"))
        ((member kind '(0 5 25 50 75 95 100))
         (concat (write-to-string kind) "th percentile"))
        (t kind)))

(defun get-x-label (plot-by)
  (declare (field-to-plot-by plot-by))
  (ecase plot-by
    (:parallelism "Parallelism degree for each node")
    (:batch-size "Output batch size")
    (:chaining "Chaining enabled?")))

(defun get-y-label (parameters time-unit)
  "Return the proper Y label name from PARAMETERS and TIME-UNIT."
  (declare (plot-parameters parameters) (string time-unit))
  (with-accessors ((kind plot-kind) (name metric)) parameters
    (ecase kind
      (:scalability "Scalability")
      (:efficiency "Efficiency")
      (:normal
       (let* ((time-unit-string (or *time-unit-y-label*
                                    (let ((abbrev (unit-to-abbrev time-unit)))
                                      (if abbrev abbrev "unknown unit"))))
              (unit-string (if (not (search "throughput"
                                            (string-downcase name)))
                               time-unit-string
                               "tuples per second")))
         (concat (string-capitalize (substitute #\Space #\- name))
                 " (" unit-string ")"))))))

(defun scale-by-base-value (base-value y measure)
  (declare (real base-value y) (string measure))
  (if (search "throughput" measure) (/ y base-value) (/ base-value y)))

(defun get-scaled-y-axis (name jsons percentile base-value)
  (declare (string name percentile) (sequence jsons) (real base-value))
  (let ((y-axis (map (type-of jsons) (lambda (j) (gethash percentile j))
                     jsons)))
    (declare (sequence y-axis))
    (map-into y-axis (lambda (y) (scale-by-base-value base-value y name))
              y-axis)))

(defun get-efficiency-y-axis (name jsons percentile base-value)
  (declare (string name percentile) (sequence jsons) (real base-value))
  (let ((scaled-y-axis (get-scaled-y-axis name jsons
                                          percentile base-value)))
    (declare (sequence scaled-y-axis))
    (loop for i below (length scaled-y-axis)
          do (setf (elt scaled-y-axis i) (/ (elt scaled-y-axis i) (1+ i)))
          finally (return scaled-y-axis))))

(defun get-y-axis (name jsons percentile plot-kind)
  (declare (string name percentile) (sequence jsons) (symbol plot-kind))
  (flet ((get-y-value (j)
           (gethash (percentile-to-dictkey percentile) j)))
    (ecase plot-kind
      (:normal
       (let* ((time-unit (gethash "time unit" (elt jsons 0)))
              (map-func
                (if (not (search name "throughput"))
                    (lambda (j) (* *y-axis-time-unit-scale* (get-y-value j)))
                    (lambda (j)
                      (* (time-unit-scale-factor (unit-to-abbrev time-unit))
                         (get-y-value j))))))
         (declare (function map-func))
         (map (type-of jsons) map-func jsons)))
      (:scalability
       (let ((base-value (get-y-value (elt jsons 0))))
         (get-scaled-y-axis name jsons percentile base-value)))
      (:efficiency
       (let ((base-value (get-y-value (elt jsons 0))))
         (get-efficiency-y-axis name jsons percentile base-value))))))

(defun get-percentile-values (percentile-map percentile-keys)
  (declare (hash-table percentile-map) (sequence percentile-keys))
  (mapcar (lambda (p) (let ((key (concat (write-to-string p) "th percentile")))
                        (gethash key percentile-map)))
          percentile-keys))

(defun get-x-axis (plot-by jsons)
  (declare (field-to-plot-by plot-by) (sequence jsons))
  (ecase plot-by
    (:parallelism (map (type-of jsons) (lambda (j)
                                         (declare (hash-table j))
                                         (first (gethash "parallelism" j)))
                       jsons))
    (:batch-size (map (type-of jsons)
                      (lambda (j)
                        (declare (hash-table j))
                        (let* ((batch-sizes (gethash "batch size" j))
                               (nonzero-batch-size (find-if-not #'zerop
                                                                batch-sizes)))
                          (if nonzero-batch-size nonzero-batch-size 0)))
                      jsons))
    (:chaining (map (type-of jsons) (lambda (x)
                                      (declare (boolean x))
                                      (if x 1 0))
                    (map (type-of jsons) (lambda (j)
                                           (declare (hash-table j))
                                           (gethash "chaining enabled" j))
                         jsons)))))

(defun chaining-to-string (chaining-p)
  (declare (boolean chaining-p))
  (if chaining-p "enabled" "disabled"))

(defun tuple-rate-to-string (rate)
  (declare (fixnum rate))
  (if (plusp rate) (write-to-string rate) "unlimited"))

(defun initial-title (parameters)
  (concat ;; (case (plot-kind parameters)
   ;;   (:scalability "Scalability for ")
   ;;   (:efficiency "Efficiency for "))
   ;; (title-from-directory (plotdir parameters))
   ;; " - "
   ;; (substitute #\Space #\-
   ;;             (string-capitalize
   ;;              (metric parameters)))
   ))

(defun title-for-plot (parameters &optional print-title)
  (declare (plot-parameters parameters))
  (let ((initial-title (if print-title (initial-title parameters) "")))
    (declare (string initial-title))
    (title-from-plotby-compareby initial-title parameters (plot-by parameters)
                                 (compare-by parameters))))

(symbol-macrolet ((chaining (chaining-to-string (chaining-p parameters)))
                  (plotdir (plotdir parameters))
                  (pardeg (write-to-string (single-pardeg parameters)))
                  (execmode (execmode parameters))
                  (timer-nodes-p (timer-nodes-p parameters))
                  (batch-size (write-to-string
                               (single-batch-size parameters)))
                  (tuple-rate (tuple-rate-to-string
                               (tuple-rate parameters))))
  (defgeneric title-from-plotby-compareby (title parameters plot-by compare-by)
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :batch-size)))
      (concat title "Chaining: " chaining
              (if (starts-with (string-downcase (namestring plotdir)) "tt")
                  (concat ", Timers: " (if timer-nodes-p
                                           "nodes"
                                           "threads"))
                  "")))
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :chaining)))
      (concat title "Batch size per node: " batch-size))
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :execmode)))
      (concat title "Chaining " chaining))
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :frequency)))
      (concat title "(Chaining: " chaining ") (generation rate: " tuple-rate
              ") (batch size per node: " batch-size))
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :timer-nodes-p)))
      (concat title "(Chaining: " chaining ") (generation rate:" tuple-rate
              ") (batch size per node: " batch-size))
    (:method (title parameters (plot-by (eql :batch-size))
              (compare-by (eql :parallelism)))
      (concat title "(Chaining " chaining ") (generation rate: "
              tuple-rate ")"))
    (:method (title parameters (plot-by (eql :batch-size))
              (compare-by (eql :chaining)))
      (concat title " (Parallelism degree per node: " pardeg
              ") (generation rate: " tuple-rate ")"))
    (:method (title parameters (plot-by (eql :batch-size))
              (compare-by (eql :execmode)))
      (concat title " (parallelism degree per node: " pardeg ") (chaining: "
              chaining ") (generation rate: " tuple-rate ")"))
    (:method (title parameters (plot-by (eql :chaining))
              (compare-by (eql :parallelism)))
      (concat title " (batch size : " batch-size
              ") (generation rate: " tuple-rate ")"))
    (:method (title parameters (plot-by (eql :chaining))
              (compare-by (eql :batch-size)))
      (concat title " (parallelism degree per node: " pardeg
              ") (generation rate: " tuple-rate ")"))
    (:method (title parameters (plot-by (eql :chaining))
              (compare-by (eql :execmode)))
      (concat title " (parallelism degree per node: " pardeg
              ") (batch size per node: " batch-size
              ") (generation rate: " tuple-rate ")"))))

(defun sort-jsons-by-parallelism (jsons)
  (declare (sequence jsons))
  (sort jsons #'< :key (lambda (j) (first (gethash "parallelism" j)))))

(defun sort-jsons-by-batch-size (jsons)
  (declare (sequence jsons))
  (sort jsons #'< :key (lambda (j) (first (gethash "batch size" j)))))

(defun sort-jsons-by-chaining (jsons)
  (declare (sequence jsons))
  (sort jsons (lambda (a b) (and a (not b)))
        :key (lambda (j) (gethash "chaining" j))))

(defun plot-from-triples (triples)
  (declare (list triples))
  (apply #'vgplot:plot triples))

(defun save-plot (name)
  (declare ((or string pathname) name))
  (vgplot:print-plot (pathname name)
                     :terminal "png size 1280,960"))

(defun boxplot-title (parameters)
  (declare (plot-parameters parameters))
  (with-accessors ((plotdir plotdir) (metric metric)
                   (compare-by compare-by)
                   (batch-size single-batch-size)
                   (timer-nodes-p timer-nodes-p)
                   (chaining-p chaining-p) (percentiles percentiles))
      parameters
    ;; (concat (title-from-directory plotdir) " - "
    ;;         (substitute #\Space #\- (string-capitalize metric))
    ;;         " (batch size: " (write-to-string batch-size)
    ;;         ") (chaining: " (if chaining-p "enabled " "disabled")
    ;;         ") (percentiles: " (write-to-string percentiles) ")"))

    (ecase compare-by
      (:batch-size (if (equalp "deterministic" (execmode parameters))
                       "DETERMINISTIC"
                       (concat "LP total latency, b = " (write-to-string batch-size)
                               ;; ", chaining: " (if chaining-p "enabled"
                               ;;                    "disabled")
                               )))
      (:chaining  (concat "MO latency, DEFAULT, b = 0, Chaining: " (if chaining-p
                                                                       "enabled"
                                                                       "disabled")
                          ;; ", Timers: " (if timer-nodes-p "nodes" "threads")
                          ))
      (:timer-nodes-p
       (concat "b = " (write-to-string batch-size)
               ", Timers: " (if timer-nodes-p "nodes" "threads"))))))

(defun emptyp (sequence)
  (declare (sequence sequence))
  (zerop (length sequence)))

(defun get-plot-triples (parameters jsons comparison-values filter-func
                         label-func)
  (declare (plot-parameters parameters) (sequence jsons)
           (list comparison-values)
           (function filter-func label-func))
  (loop with plot-triples
        for comparison-value in comparison-values
        for current-jsons = (funcall filter-func jsons comparison-value)
        unless (emptyp current-jsons)
          do (let ((x-axis (get-x-axis (plot-by parameters) current-jsons))
                   (y-axis (get-y-axis (metric parameters) current-jsons
                                       (percentile parameters)
                                       (plot-kind parameters)))
                   (label (funcall label-func comparison-value)))
               (push label plot-triples)
               (push y-axis plot-triples)
               (push x-axis plot-triples))
        finally (return plot-triples)))

(defun get-triples-comparing-by (parameters jsons compare-by)
  (declare (plot-parameters parameters) (sequence jsons) (symbol compare-by))
  (ecase compare-by
    (:parallelism
     (get-plot-triples parameters jsons (pardegs parameters)
                       #'filter-jsons-by-parallelism
                       (lambda (value)
                         (concat "p = "
                                 (write-to-string value)))))
    (:batch-size
     (get-plot-triples parameters jsons (batch-sizes parameters)
                       #'filter-jsons-by-batch-size
                       (lambda (value)
                         (concat "b = " (write-to-string value)))))
    (:chaining
     (get-plot-triples parameters jsons '(nil t)
                       #'filter-jsons-by-chaining
                       (lambda (value)
                         (concat "Chaining: " (chaining-to-string value)))))
    (:execmode
     (get-plot-triples parameters jsons '("deterministic" "default")
                       #'filter-jsons-by-execmode
                       (lambda (value) (string-upcase value))))
    (:frequency
     (get-plot-triples parameters jsons (frequencies parameters)
                       #'filter-jsons-by-frequency
                       (lambda (value)
                         (concat "Output frequency for all operators: "
                                 (write-to-string value)))))
    (:timer-nodes-p
     (get-plot-triples parameters jsons '(nil t)
                       #'filter-jsons-by-timernode-impl
                       (lambda (value)
                         (concat "Timer nodes used: "
                                 (write-to-string value)))))))

(defun get-additional-triples (length plot-kind)
  (ecase plot-kind
    (:scalability (let* ((x-axis (loop for i from 1 to length collect i))
                         (y-axis x-axis)
                         (label "Ideal"))
                    (values x-axis y-axis label)))
    (:efficiency (let ((x-axis (loop for i from 1 to length collect i))
                       (y-axis (loop repeat length collect 1))
                       (label "Ideal"))
                   (values x-axis y-axis label)))))

(defun get-triples (parameters &optional jsons)
  (declare (plot-parameters parameters) (sequence jsons))
  (unless jsons
    (setf jsons (get-json-objs-from-directory (plotdir parameters))
          jsons (filter-jsons parameters jsons))
    (when (emptyp jsons)
      (format t "No data found with the specified parameters, not plotting...")
      (return-from get-triples)))
  (let ((actual-triples (get-triples-comparing-by parameters jsons
                                                  (compare-by parameters))))
    (if (eql (plot-kind parameters) :normal)
        actual-triples
        (let ((length (length (second actual-triples))))
          (multiple-value-bind (x-axis y-axis label)
              (get-additional-triples length (plot-kind parameters))
            (nconc (list x-axis y-axis label) actual-triples))))))

(defun plot (&optional (parameters *default-plot-parameters*) jsons
               image-path)
  (declare (plot-parameters parameters) (sequence jsons)
           ((or null pathname string) image-path))
  (when (and (emptyp jsons)
             (not (equal "" (plotdir parameters))))
    (setf jsons (get-json-objs-from-directory (plotdir parameters))))
  (setf jsons (filter-jsons parameters jsons))
  (when (and (emptyp jsons)
             (null (extra-triples parameters)))
    (format t "No data found with the specified parameters, not plotting...")
    (return-from plot))
  (let ((plot-triples (nconc (get-triples parameters jsons)
                             (extra-triples parameters))))
    (when *debug*
      (print (reverse plot-triples)))
    (plot-from-triples plot-triples))
  (vgplot:title (if (print-title-p parameters)
                    (title-for-plot parameters t)
                    ""))
  (let* ((xlabel (get-x-label (plot-by parameters)))
         (time-unit (gethash "time unit" (elt jsons 0)))
         (ylabel (get-y-label parameters time-unit)))
    (vgplot:xlabel xlabel)
    (vgplot:ylabel ylabel))
  (vgplot:grid t)
  (when image-path
    (ensure-directories-exist image-path :verbose *debug*)
    (save-plot image-path)))

(defun get-csv-fields (parameters jsons)
  (declare (plot-parameters parameters)
           (sequence jsons))
  (loop with original-triples = (nconc (get-triples parameters jsons)
                                       (extra-triples parameters))
        and x-axis-prefix = (ecase (plot-by parameters)
                              (:parallelism "p = ")
                              (:batch-size "b = "))
        with x-axis = (cons "" (mapcar (lambda (x)
                                         (concatenate 'string
                                                      x-axis-prefix
                                                      (write-to-string x)))
                                       (coerce (first original-triples)
                                               'list)))
        for triple-list = original-triples then (cdddr triple-list)
        while triple-list
        collect (cons (third triple-list)
                      (coerce (second triple-list) 'list))
          into result
        finally (return (transpose-lists
                         (cons x-axis (nreverse result))))))

(defun write-triples-to-csv (stream &optional (parameters *default-plot-parameters*)
                                      jsons)
  (declare (plot-parameters parameters)
           (sequence jsons))
  (unless jsons
    (setf jsons (get-json-objs-from-directory (plotdir parameters))))
  (setf jsons (filter-jsons parameters jsons))
  (when (emptyp jsons)
    (format t "No data found with the specified parameters, not plotting...")
    (return-from write-triples-to-csv))
  (let ((fields (get-csv-fields parameters jsons)))
    (cl-csv:write-csv fields :stream stream)))

(defun draw-boxplot (&key title x-label y-label y-axis x-axis)
  (declare (string x-label y-label)
           ((or string null) title)
           (sequence x-axis y-axis))
  ;; (py4cl:import-module "matplotlib.pyplot" :as "plt")
  (plt:boxplot y-axis :positions x-axis)
  ;; (plt:figure)
  (plt:xlabel x-label
              ;; :fontsize 12
              )
  (plt:ylabel y-label
              ;; :fontsize 16
              )
  (when title
    (plt:title title :loc "center"
                     ;; :fontsize 16
                     ))
  (plt:grid t)
  ;; (plt:show)
  ;; (plt:close "all")
  )

(defun boxplot (&optional (parameters *default-plot-parameters*) jsons
                  image-path (title-p t))
  (declare (plot-parameters parameters) (sequence jsons)
           ((or null pathname string) image-path) )
  (unless jsons
    (setf jsons (get-json-objs-from-directory (plotdir parameters))))
  (setf jsons (filter-jsons-by-name jsons (metric parameters))
        jsons (filter-jsons-by-chaining jsons (chaining-p parameters))
        jsons (filter-jsons-by-batch-size jsons (single-batch-size parameters))
        jsons (filter-jsons-by-sampling-rate jsons (sampling-rate parameters))
        jsons (filter-jsons-by-tuple-rate jsons (tuple-rate parameters))
        jsons (filter-jsons-by-execmode jsons (execmode parameters)))
  (when (some #'contains-timernode-impl-fields-p jsons)
    (setf jsons (filter-jsons-by-timernode-impl jsons (timer-nodes-p parameters))))
  (when (emptyp jsons)
    (format t "No data found with the specified parameters, not plotting...")
    (return-from boxplot))
  (setf jsons (sort-jsons-by-parallelism jsons))
  (let ((time-unit (gethash "time unit" (elt jsons 0))))
    (let ((title (if title-p (boxplot-title parameters) nil))
          (xlabel (get-x-label (plot-by parameters)))
          (ylabel (get-y-label parameters time-unit))
          (x-axis (get-x-axis (plot-by parameters) jsons))
          (y-axis (map (type-of jsons)
                       (lambda (j)
                         (get-percentile-values j (percentiles parameters)))
                       jsons)))
      (map-into y-axis (lambda (percentiles)
                         (map-into percentiles
                                   (lambda (y) (* y *y-axis-time-unit-scale*))
                                   percentiles))
                y-axis)
      (when *debug*
        (format t "x-axis: ~a~%y-axis: ~a~%" x-axis y-axis))
      (draw-boxplot :title title :x-label xlabel :y-label ylabel
                    :x-axis x-axis :y-axis y-axis))))

(defun get-image-file-name (parameters)
  (declare (plot-parameters parameters))
  (let ((output-file-name
          (concat (metric parameters) "-"
                  (string-downcase (symbol-name (plot-kind parameters)))
                  "-by-" (string-downcase (symbol-name (plot-by parameters)))
                  "-compare-by-"
                  (string-downcase (symbol-name (compare-by parameters)))
                  (case (compare-by parameters)
                    (:batch-size
                     (concat "-chaining-"
                             (chaining-to-string (chaining-p parameters))))
                    (:chaining
                     (concat "-batchsize-"
                             (write-to-string (single-batch-size parameters))))
                    (otherwise ""))
                  (if (starts-with (namestring (plotdir parameters))
                                   "tt-trending-topics")
                      (concat "-timernodes-" (if (timer-nodes-p parameters)
                                                 "yes"
                                                 "no"))
                      "")
                  ".png")))
    (pathname (concat (namestring (base-dirname (plotdir parameters)))
                      "/graphs/" output-file-name))))

(defun get-metric-list (parameters)
  (declare (plot-parameters parameters))
  (if (starts-with (namestring (plotdir parameters)) "lp-log-processing")
      (list "throughput" "service time" "total-latency")
      (list "throughput" "service time" "latency")))

(defun get-timer-nodes-mode-list (parameters)
  (declare (plot-parameters parameters))
  (if (starts-with (namestring (plotdir parameters)) "tt-trending-topics")
      (list nil t)
      (list nil)))

(defun generate-images (&optional (parameters *default-plot-parameters*))
  (declare (plot-parameters parameters))
  (with-accessors ((plot-by plot-by) (plotdir plotdir)
                   (compare-by compare-by) (plot-kind plot-kind))
      parameters
    (when (starts-with (namestring plotdir) "mo-machine-outlier")
      (generate-images-comparing-execmode parameters))
    (let ((jsons (get-json-objs-from-directory plotdir))
          (timer-nodes-mode-list (get-timer-nodes-mode-list parameters))
          (metrics (get-metric-list parameters)))
      (dolist (timer-nodes-p timer-nodes-mode-list)
        (setf (timer-nodes-p parameters) timer-nodes-p)
        (dolist (current-plot-by '(:parallelism :batch-size :chaining))
          (setf plot-by current-plot-by)
          (dolist (current-compare-by (remove current-plot-by '(:parallelism
                                                                :batch-size
                                                                :chaining)))
            (when *debug*
              (print (list current-plot-by current-compare-by)))
            (setf compare-by current-compare-by)
            (dolist (metric metrics)
              (setf (metric parameters) metric)
              (dolist (plot-kind '(:normal :scalability :efficiency))
                (setf (plot-kind parameters) plot-kind)
                (unless (eql current-plot-by :chaining)
                  (dolist (chaining-p '(nil t))
                    (setf (chaining-p parameters) chaining-p)
                    (let ((image-path (get-image-file-name parameters)))
                      (plot parameters jsons image-path))))))))))))

(defun generate-images-comparing-execmode (parameters)
  (declare (plot-parameters parameters))
  (with-accessors ((plot-by plot-by) (plotdir plotdir)
                   (compare-by compare-by) (plot-kind plot-kind))
      parameters
    (let ((jsons (get-json-objs-from-directory plotdir)))
      (setf compare-by :execmode)
      (dolist (current-plot-by '(:parallelism :chaining))
        (setf plot-by current-plot-by)
        (dolist (metric '("throughput" "service time" "latency"))
          (setf (metric parameters) metric)
          (dolist (current-plot-kind '(:normal :scalability :efficiency))
            (setf plot-kind current-plot-kind)
            (unless (eql current-plot-by :chaining)
              (dolist (chaining-p '(nil t))
                (setf (chaining-p parameters) chaining-p)
                (let ((image-path (get-image-file-name parameters)))
                  (plot parameters jsons image-path))))))))))

(defun square-size-string (size)
  (declare (fixnum size))
  (let ((size-string (write-to-string size)))
    (concatenate 'string size-string "," size-string)))

(defun plot-subplots (plot-func parameters)
  (vgplot:close-all-plots)
  (vgplot:format-plot t "set format y '%.0f'")
  (let* ((subplot-rows 2)
         (subplot-columns (/ (length parameters) 2))
         (width (if (= 4 (length parameters)) 900 600))
         (height (case (length parameters)
                   (1 400)
                   (2 600)
                   (4 700)))
         (title-size (if (= 4 (length parameters)) 13 11))
         (xlabel-size (if (= 4 (length parameters)) 11 9))
         (ylabel-size (if (= 4 (length parameters)) 12 11))
         (ytics-size (if (= 4 (length parameters)) 10 10))
         (legend-size (if (= 4 (length parameters)) 12 10)))
    (vgplot:format-plot t (concat "set terminal qt size "
                                  (write-to-string width)
                                  ","
                                  (write-to-string height)))
    (vgplot:format-plot t (concat "set title offset 0.0,-0.5 font \","
                                  (write-to-string title-size)
                                  "\""))
    (vgplot:format-plot t "set ylabel offset -0.5,0.0 font \","
                        (write-to-string ylabel-size)
                        "\"")
    (vgplot:format-plot t (concat "set xlabel offset 0.0,0.2 font \","
                                  (write-to-string xlabel-size)
                                  "\""))
    (vgplot:format-plot t (concat "set key font \","
                                  (write-to-string legend-size)
                                  "\""))
    (vgplot:format-plot t (concat "set ytics font \","
                                  (write-to-string ytics-size)
                                  "\""))
    (ecase (length parameters)
      (1
       (vgplot:legend :outside :boxon :southeast)
       (funcall plot-func (first parameters)))
      ((2 4)
       (loop for parameter in parameters
             and i from 0
             do (vgplot:subplot subplot-rows subplot-columns i)
                (vgplot:legend :outside :boxon :southeast)
                (funcall plot-func parameter))))))

(defun plot-subplots-from-parameters (&rest parameters)
  (plot-subplots (lambda (parameter) (plot parameter nil nil))
                 parameters))

(defun plot-subplots-from-raw-data (&rest raw-data-list)
  (plot-subplots (lambda (raw-data)
                   (apply #'vgplot:plot (triples raw-data))
                   (vgplot:title (title raw-data))
                   (vgplot:xlabel (x-label raw-data))
                   (vgplot:ylabel (y-label raw-data)))
                 raw-data-list))

(defun boxplot-subplots (&rest parameters)
  (plt:close "all")
  (ecase (length parameters)
    (1
     (boxplot (first parameters) nil nil t)
     (plt:show))
    ((2 4)
     (let ((subplot-dimension 2)
           (figsize (if (= 2 (length parameters))
                        (list 12.5 7)
                        (list 9 7))))
       (plt:subplots subplot-dimension
                     subplot-dimension
                     :figsize figsize)
       (plt:subplots_adjust :wspace 0.3 :hspace 0.4)
       (loop for parameter in parameters
             and i from 1
             do (plt:subplot subplot-dimension (/ (length parameters) 2) i)
                (plt:ticklabel_format :style "plain")
                (boxplot parameter nil nil t)
                (when (= 2 (length parameters))
                  (plt:xticks :fontsize 11)
                  (plt:yticks :fontsize 12))
             finally (plt:show))))))
