package hu.infokristaly.homework4websocketserver.cv;

import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_video.BackgroundSubtractorMOG2;

import static org.bytedeco.opencv.global.opencv_core.countNonZero;
import static org.bytedeco.opencv.global.opencv_video.createBackgroundSubtractorMOG2;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;

public class AdvancedMotionDetector {
    private final BackgroundSubtractorMOG2 backSub;
    private final Mat foregroundMask;

    public AdvancedMotionDetector() {
        // history: 500 képkockára emlékszik vissza
        // varThreshold: 16 (minél kisebb, annál érzékenyebb)
        // detectShadows: true (az árnyékokat szürkével jelöli, nem fehérrel)
        this.backSub = createBackgroundSubtractorMOG2(500, 16, true);
        this.foregroundMask = new Mat();
    }

    public boolean detectMotion(byte[] imageData) {
        Mat frame = imdecode(new Mat(imageData), IMREAD_COLOR);
        if (frame.empty()) return false;
        return detectMotionFromMat(frame);
    }

    public boolean detectMotionFromMat(Mat frame) {
        // 1. Háttér kivonása (itt történik a "mágia")
        // A 'learningRate' -1-en hagyása automatikus tanulást jelent
        backSub.apply(frame, foregroundMask);

        // 2. Tisztítás: az árnyékok (szürke) eltávolítása, csak a valódi mozgás (fehér) maradjon
        threshold(foregroundMask, foregroundMask, 200, 255, THRESH_BINARY);

        // 3. Morfológiai műveletek: apró zajpontok eltüntetése
        Mat kernel = getStructuringElement(MORPH_RECT, new Size(3, 3));
        morphologyEx(foregroundMask, foregroundMask, MORPH_OPEN, kernel);

        // 4. Mozgás mértékének ellenőrzése (fehér pixelek száma)
        int whitePixels = countNonZero(foregroundMask);

        // Ha a kép x százaléka megváltozott, mozgást jelzünk
        double motionPercentage = (double) whitePixels / (frame.rows() * frame.cols());

        return motionPercentage > 0.01; // 1%-os változás felett riaszt
    }
}